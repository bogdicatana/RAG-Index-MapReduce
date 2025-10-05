package rag

import io.circe.parser.parse
import org.apache.hadoop.fs.{FileSystem, Path as HPath}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.store.FSDirectory
import org.slf4j.LoggerFactory

import java.io.{IOException, PrintWriter}
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*
import scala.util.Using
import util.Settings

class ShardReducer extends Reducer[IntWritable, Text, Text, Text] {
    private val logger = LoggerFactory.getLogger(classOf[ShardReducer])

    override def reduce(
                           key: IntWritable,
                           values: java.lang.Iterable[Text],
                           context: Reducer[IntWritable, Text, Text, Text]#Context
                       ): Unit = {

        val shard = key.get()
        logger.info(s"Starting reduce for shard $shard")
        val localDir: Path = Files.createTempDirectory(s"lucene-shard-$shard")
        val dir = FSDirectory.open(localDir)
        val iwConfig = new IndexWriterConfig(new StandardAnalyzer())
        val iw = new IndexWriter(dir, iwConfig)

        // --- Parse documents safely ---
        case class ParsedDoc(id: String, chunkId: String, text: String, vec: Array[Float], doc: Document)

        // --- Parse documents safely ---
        val parsedDocs: Seq[ParsedDoc] = values.asScala.flatMap { t =>
            parse(t.toString).toOption.flatMap { jsonVal =>
                val cursor = jsonVal.hcursor
                try {
                    val docId = cursor.get[String]("doc_id").getOrElse("")
                    val chunkId = cursor.get[Int]("chunk_id").map(_.toString).getOrElse("")
                    val text = cursor.get[String]("text").getOrElse("")
                    val vec = cursor.get[Vector[Float]]("vec").getOrElse(Vector.empty).toArray

                    val doc = new Document()
                    doc.add(new StringField("doc_id", docId, Field.Store.YES))
                    doc.add(new StringField("chunk_id", chunkId, Field.Store.YES))
                    doc.add(new TextField("text", text, Field.Store.YES))
                    doc.add(new KnnFloatVectorField("vec", vec, VectorSimilarityFunction.COSINE))
                    iw.addDocument(doc)
                    Some(ParsedDoc(docId, chunkId, text, vec, doc))
                } catch {
                    case e: Exception =>
                        logger.warn(s"Failed to parse doc: ${t.toString.take(200)}", e)
                        None
                }
            }
        }.toSeq

        iw.close()
        dir.close()

        logger.info(s"Shard $shard parsed ${parsedDocs.size} documents")

        val hadoopConf = context.getConfiguration
        val fs = FileSystem.get(hadoopConf)
        val outputPathStr = hadoopConf.get("mapreduce.output.fileoutputformat.outputdir")
        if (outputPathStr == null) throw new IOException("Output directory not set")
        val hdfsIndexPath = new HPath(outputPathStr, s"index_shard_$shard")
        copyLocalToHdfs(localDir, hdfsIndexPath, fs)
        context.write(new Text(shard.toString), new Text(hdfsIndexPath.toString))
        logger.info(s"Shard $shard Lucene index written to $hdfsIndexPath")


        // --- Statistics ---
        val statsDir = new HPath(outputPathStr, s"stats_shard_$shard")
        fs.mkdirs(statsDir)

        // 1️⃣ Vocabulary
        val tokenized: Seq[String] = parsedDocs.flatMap { doc =>
            doc.text.toLowerCase
                .replaceAll("[^a-z0-9\\s]", " ")
                .split("\\s+")
                .filter(w => w.nonEmpty && w.length >= 2 && w.length <= 25)
        }
        val vocabCounts = tokenized.groupBy(identity).view.mapValues(_.size.toLong).toMap
        val sortedVocab = vocabCounts.toSeq.sortBy(-_._2)

        logger.info(s"Shard $shard wrote ${sortedVocab.size} vocab entries")

        Using.resource(new PrintWriter(fs.create(new HPath(statsDir, s"vocab_shard_$shard.csv")))) { out =>
            out.println("word,freq")
            sortedVocab.foreach { case (w, f) => out.println(s"$w,$f") }
        }

        // 2️⃣ Approximate token embeddings: average chunk vectors where token occurs
        val tokenVectors: Map[String, Array[Float]] = parsedDocs.flatMap { doc =>
                doc.text.toLowerCase
                    .replaceAll("[^a-z0-9\\s]", " ")
                    .split("\\s+")
                    .filter(w => w.nonEmpty && w.length >= 2 && w.length <= 25)
                    .map(token => token -> doc.vec)
            }
            .groupBy(_._1)
            .map { case (token, occurrences) =>
                val vectors = occurrences.map(_._2)
                val avgVec = vectors.transpose.map(arr => arr.sum / arr.length).toArray
                token -> avgVec
            }

        def cosine(a: Array[Float], b: Array[Float]): Double = {
            val dot = a.zip(b).map { case (x, y) => x * y }.sum
            val na = math.sqrt(a.map(x => x * x).sum)
            val nb = math.sqrt(b.map(x => x * x).sum)
            if (na == 0.0 || nb == 0.0) 0.0 else dot / (na * nb)
        }

        // 3️⃣ Select top frequent tokens for neighbors / similarity / analogies
        val topTokens = sortedVocab.take(50).map(_._1).filter(tokenVectors.contains)

        // Nearest neighbors
        val neighborLines: Seq[String] =
            "token,neighbor,similarity" +:
                topTokens.flatMap { token =>
                    tokenVectors.toSeq
                        .filter(_._1 != token)
                        .map { case (tok2, vec2) => tok2 -> cosine(tokenVectors(token), vec2) }
                        .sortBy(-_._2)
                        .take(5)
                        .map { case (neighbor, sim) => s"$token,$neighbor,$sim" }
                }

        Using.resource(new PrintWriter(fs.create(new HPath(statsDir, s"neighbors_shard_$shard.csv")))) { out =>
            neighborLines.foreach(out.println)
        }
        logger.info(s"Shard $shard wrote ${neighborLines.size - 1} neighbor entries")

        // Similarity pairs
        val similarityPairs = try {
            Settings.similarityPairs
        } catch {
            case e: Exception =>
                logger.warn("Failed to load similarity pairs from Settings, using empty set", e)
                Seq.empty[(String, String)]
        }

        val simLines =
            "word1,word2,similarity" +:
                similarityPairs.flatMap { case (a, b) =>
                    for {
                        va <- tokenVectors.get(a)
                        vb <- tokenVectors.get(b)
                    } yield s"$a,$b,${cosine(va, vb)}"
                }

        Using.resource(new PrintWriter(fs.create(new HPath(statsDir, s"similarities_shard_$shard.csv")))) { out =>
            simLines.foreach(out.println)
        }

        logger.info(s"Shard $shard wrote ${simLines.size - 1} similarity entries")

        // Analogy triplets: a:b :: c:?
        val analogyTriplets = try {
            Settings.analogyTriplets
        } catch {
            case e: Exception =>
                logger.warn("Failed to load analogy triplets from SettingsUtil, using empty set", e)
                Seq.empty[(String, String, String)]
        }

        val anaLines =
            "a,b,c,predicted,score" +:
                analogyTriplets.flatMap { case (a, b, c) =>
                    for {
                        va <- tokenVectors.get(a)
                        vb <- tokenVectors.get(b)
                        vc <- tokenVectors.get(c)
                        analogy = va.zip(vb).zip(vc).map { case ((xa, xb), xc) => xa - xb + xc }
                        best = tokenVectors
                            .view
                            .filterKeys(k => k != a && k != b && k != c)
                            .map { case (tok, vec) => tok -> cosine(analogy, vec) }
                            .maxByOption(_._2)
                    } yield s"$a,$b,$c,${best.map(_._1).getOrElse("(none)")},${best.map(_._2).getOrElse(0.0)}"
                }

        Using.resource(new PrintWriter(fs.create(new HPath(statsDir, s"analogies_shard_$shard.csv")))) { out =>
            anaLines.foreach(out.println)
        }

        logger.info(s"Shard $shard wrote ${anaLines.size - 1} analogy entries")


        context.write(new Text(s"stats_shard_$shard"), new Text("completed"))
        logger.info(s"Shard $shard completed successfully")

    }




    private def copyLocalToHdfs(localDir: Path, hdfsPath: HPath, fs: FileSystem): Unit = {
        if (fs.exists(hdfsPath)) {
            fs.delete(hdfsPath, true)
        }
        fs.mkdirs(hdfsPath)

        Files.list(localDir).forEach { filePath =>
            val fileName = filePath.getFileName.toString
            val destPath = new HPath(hdfsPath, fileName)
            val inputStream = Files.newInputStream(filePath)
            val outputStream = fs.create(destPath)

            try {
                val buffer = new Array[Byte](4096)
                Iterator
                    .continually(inputStream.read(buffer))
                    .takeWhile(_ != -1)
                    .foreach(readBytes => outputStream.write(buffer, 0, readBytes))
            } finally {
                inputStream.close()
                outputStream.close()
            }
        }
    }
}
