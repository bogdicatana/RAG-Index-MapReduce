package rag

import java.nio.file.{Files, Path}
import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import io.circe.parser.parse
import io.circe.HCursor

import org.apache.lucene.document._
import org.apache.lucene.index._
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.VectorSimilarityFunction
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import util.Settings

import java.io.PrintWriter
import scala.util.{Try, Using}

class ShardReducer extends Reducer[IntWritable, Text, Text, Text] {
    private val logger = LoggerFactory.getLogger(classOf[ShardReducer])

    override def reduce(
                           key: IntWritable,
                           values: java.lang.Iterable[Text],
                           context: Reducer[IntWritable, Text, Text, Text]#Context
                       ): Unit = {

        val shard = key.get()
        val localDir: Path = Files.createTempDirectory(s"lucene-shard-$shard")
        val dir = FSDirectory.open(localDir)
        val iwConfig = new IndexWriterConfig(new StandardAnalyzer())
        val iw = new IndexWriter(dir, iwConfig)

        // --- Parse documents safely ---
        case class ParsedDoc(id: String, chunkId: String, text: String, vec: Array[Float], doc: Document)

        val parsedDocs: Seq[ParsedDoc] = values.asScala.flatMap { t =>
            parse(t.toString).toOption.flatMap { jsonVal =>
                val cursor = jsonVal.hcursor
                for {
                    docId <- cursor.get[String]("doc_id").toOption
                    chunkId <- cursor.get[Int]("chunk_id").map(_.toString).toOption
                    text <- cursor.get[String]("text").toOption
                    vec <- cursor.get[Vector[Float]]("vec").map(_.toArray).toOption
                } yield {
                    val doc = new Document()
                    doc.add(new StringField("doc_id", docId, Field.Store.YES))
                    doc.add(new StringField("chunk_id", chunkId, Field.Store.YES))
                    doc.add(new TextField("text", text, Field.Store.YES))
                    doc.add(new KnnFloatVectorField("vec", vec, VectorSimilarityFunction.COSINE))
                    iw.addDocument(doc)
                    ParsedDoc(docId, chunkId, text, vec, doc)
                }
            }
        }.toSeq

        iw.close()
        dir.close()

        val hadoopConf = context.getConfiguration
        val fs = FileSystem.get(hadoopConf)
        val outputPathStr = hadoopConf.get("mapreduce.output.fileoutputformat.outputdir")
        if (outputPathStr == null) throw new IOException("Output directory not set")
        val hdfsIndexPath = new HPath(outputPathStr, s"index_shard_$shard")
        copyLocalToHdfs(localDir, hdfsIndexPath, fs)
        context.write(new Text(shard.toString), new Text(hdfsIndexPath.toString))

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

        // Similarity pairs: pair each top token with its next token in frequency
        // Hardcoded similarity pairs
        val similarityPairs = Seq(
            ("function", "variable"),
            ("type", "variable"),
            ("addition", "deletion"),
            ("program", "software"),
            ("AST", "structure")
        )

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

        // Analogy triplets: a:b :: c:? using top tokens sliding window
        // Hardcoded analogy triplets
        val analogyTriplets = Seq(
            ("function", "variable", "type"),
            ("addition", "deletion", "change"),
            ("program", "software", "repository"),
            ("AST", "structure", "diff")
        )

        val anaLines =
            "a,b,c,predicted,score" +:
                analogyTriplets.flatMap { case (a, b, c) =>
                    for {
                        va <- tokenVectors.get(a)
                        vb <- tokenVectors.get(b)
                        vc <- tokenVectors.get(c)
                        analogy = va.zip(vb).zip(vc).map { case ((xa, xb), xc) => xa - xb + xc }
                        best = tokenVectors
                            .filterKeys(k => k != a && k != b && k != c)
                            .map { case (tok, vec) => tok -> cosine(analogy, vec) }
                            .maxByOption(_._2)
                    } yield s"$a,$b,$c,${best.map(_._1).getOrElse("(none)")},${best.map(_._2).getOrElse(0.0)}"
                }

        Using.resource(new PrintWriter(fs.create(new HPath(statsDir, s"analogies_shard_$shard.csv")))) { out =>
            anaLines.foreach(out.println)
        }

        context.write(new Text(s"stats_shard_$shard"), new Text("completed"))
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
