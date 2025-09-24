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

        try {
            // Process each JSON record
            values.asScala.foreach { t =>
                val json = t.toString
                val doc = parse(json) match {
                    case Left(err) =>
                        logger.error(s"Failed to parse JSON: $json", err)
                        null
                    case Right(jsonVal) =>
                        val cursor: HCursor = jsonVal.hcursor
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

                            doc
                        } catch {
                            case ex: Exception =>
                                logger.error(s"Error extracting fields from JSON: $json", ex)
                                null
                        }
                }
                if (doc != null) {
                    iw.addDocument(doc)
                }
            }

            iw.commit()
        } finally {
            iw.close()
            dir.close()
        }

        // Copy local index dir to HDFS: <job-output>/index_shard_<shard>
        val hadoopConf = context.getConfiguration
        val fs = FileSystem.get(hadoopConf)

        // Assuming the job output directory is set as an output path in the job config
        val outputPathStr = hadoopConf.get("mapreduce.output.fileoutputformat.outputdir")
        if (outputPathStr == null) {
            logger.error("Output directory not set in configuration")
            throw new IOException("Output directory not set")
        }

        val hdfsIndexPath = new HPath(outputPathStr, s"index_shard_$shard")

        // Copy all files from localDir to hdfsIndexPath
        copyLocalToHdfs(localDir, hdfsIndexPath, fs)

        // Optionally, emit the shard path as output (key = shard id as string, value = HDFS index path)
        context.write(new Text(shard.toString), new Text(hdfsIndexPath.toString))
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
                val buffer = new Array[Byte](4000)
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
