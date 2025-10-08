import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{DirectoryReader, MultiReader}
import org.apache.lucene.queryparser.classic.{QueryParser, QueryParserBase}
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.lucene.store.FSDirectory
import org.slf4j.LoggerFactory
import util.*
import rag.*
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Files
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

object Main {
    private val logger = LoggerFactory.getLogger("Main")

    def main(args: Array[String]): Unit = {

        val input = Settings.inputPDFS
        val output = Settings.output.outputDir
        val queryText = Settings.ollama.query

        // === 1️⃣ Setup environment ===

        val conf = new Configuration()
        conf.set("fs.defaultFS", "file:///")
        conf.set("mapreduce.framework.name", "local")
//        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
//        conf.set("mapreduce.job.local.address", "127.0.0.1")
//        conf.set("mapreduce.jobtracker.address", "127.0.0.1:8021")
//        conf.set("yarn.resourcemanager.hostname", "127.0.0.1")
//        conf.set("yarn.nodemanager.hostname", "127.0.0.1")
//        conf.set("mapreduce.map.java.opts", "-Dsun.net.spi.nameservice.provider.1=default")
//        conf.set("mapreduce.reduce.java.opts", "-Dsun.net.spi.nameservice.provider.1=default")
        conf.setInt("mapreduce.local.map.tasks.maximum", Settings.numMappers)
        logger.info("💻 Running in local mode.")

        val fs = FileSystem.get(conf)
        val outputPath = new Path(output)

        // === 2️⃣ Skip if output already exists ===
        if (fs.exists(outputPath) && fs.listStatus(outputPath).nonEmpty) {
            logger.info(s"⚡ Output directory '$output' already exists. Skipping MapReduce job.")
        } else {
            logger.info(s"🚀 Running MapReduce indexing job on $input → $output")

            val job = Job.getInstance(conf, "rag-index-builder")
            job.setJarByClass(classOf[RagMapper])
            job.setMapperClass(classOf[RagMapper])
            job.setReducerClass(classOf[ShardReducer])
            job.setMapOutputKeyClass(classOf[IntWritable])
            job.setMapOutputValueClass(classOf[Text])
            job.setOutputKeyClass(classOf[Text])
            job.setOutputValueClass(classOf[Text])
            job.setNumReduceTasks(Settings.numReduceJobs)
            job.setInputFormatClass(classOf[NLineInputFormat])

            FileInputFormat.setInputDirRecursive(job, true)
            FileInputFormat.addInputPaths(job, input)
            NLineInputFormat.setNumLinesPerSplit(job, Settings.pdfsPerSplit)
            FileOutputFormat.setOutputPath(job, outputPath)

            val success = job.waitForCompletion(true)
            if (!success) {
                logger.error("❌ MapReduce job failed.")
                sys.exit(1)
            } else {
                logger.info("✅ MapReduce indexing completed successfully!")
            }
        }

        // === 3️⃣ Merge per-shard statistics ===
        logger.info("📊 Merging shard-level statistics...")
        mergeStats(output)
        logger.info("✅ Merged statistics created.")

        // === 4️⃣ Load Lucene index shards ===
        logger.info("🔍 Loading Lucene index shards...")
        val indexDirs = new File(output).listFiles().filter(_.getName.startsWith("index_shard_"))
        val analyzer = new StandardAnalyzer()
        val readers = indexDirs.flatMap { d =>
            try Some(DirectoryReader.open(FSDirectory.open(d.toPath)))
            catch {
                case e: Exception =>
                    logger.warn(s"⚠️ Could not open index shard ${d.getName}: ${e.getMessage}")
                    None
            }
        }

        // Explicitly create Array[DirectoryReader]
        val multiReader = new MultiReader(readers.toArray[DirectoryReader]*)
        val searcher = new IndexSearcher(multiReader)
        val parser = new QueryParser("text", analyzer)
        val query: Query = parser.parse(QueryParserBase.escape(queryText))

        val topDocs = searcher.search(query, Settings.ollama.topResults)
        logger.info(s"🔎 Found ${topDocs.scoreDocs.length} results for query: '$queryText'")

        topDocs.scoreDocs.foreach { sd =>
            val doc = searcher.storedFields().document(sd.doc)
            logger.info(s"Result [${sd.score}]: ${doc.get("doc_id")} chunk ${doc.get("chunk_id")}")
            logger.debug(s"Text snippet: ${doc.get("text").take(200)}...")
        }

        // === 5️⃣ Summarize with Ollama ===
        val ollamaClient = new Ollama(Settings.ollama.host)
        val contextText = topDocs.scoreDocs.map(sd => searcher.storedFields().document(sd.doc).get("text")).mkString("\n---\n")

        val messages = Vector(
            "system" -> "You are an assistant that summarizes information from software engineering research papers.",
            "user" -> s"Given the following context, answer the question:\n\n$contextText\n\nQuestion: $queryText"
        )

        logger.info("🧠 Querying Ollama for summary...")
        val answer = ollamaClient.chat(messages, Settings.ollama.chatModel)
        logger.info(s"💬 Ollama Answer:\n$answer")

        // Cleanup
        readers.foreach(_.close())
    }

    /** Merge CSV stats from all shards */
    private def mergeStats(outputDir: String): Unit = {
        val statsDir = new File(outputDir)
        val mergedDir = new File(statsDir, "merged-stats")
        mergedDir.mkdirs()

        // --- 1️⃣ Merge vocab by summing frequencies ---
        val vocabCounts = scala.collection.mutable.Map[String, Long]()
        val shardDirs = statsDir.listFiles().filter(f => f.isDirectory && f.getName.startsWith("stats_shard_"))

        shardDirs.foreach { shard =>
            val vocabFile = new File(shard, s"vocab_shard_${shard.getName.split("_").last}.csv")
            if (vocabFile.exists()) {
                val lines = Files.readAllLines(vocabFile.toPath).asScala
                lines.drop(1).foreach { line =>
                    val parts = line.split(",", 2)
                    if (parts.length == 2) {
                        val word = parts(0).trim
                        val freq = parts(1).trim.toLongOption.getOrElse(0L)
                        vocabCounts.update(word, vocabCounts.getOrElse(word, 0L) + freq)
                    }
                }
            }
        }

        val sortedVocab = vocabCounts.toSeq.sortBy(-_._2)
        val vocabWriter = new BufferedWriter(new FileWriter(new File(mergedDir, "vocab.csv")))
        vocabWriter.write("word,freq\n")
        sortedVocab.foreach { case (w, f) => vocabWriter.write(s"$w,$f\n") }
        vocabWriter.close()

        // --- 2️⃣ Merge other CSVs by concatenation ---
        val otherFiles = Seq("neighbors", "similarities", "analogies")

        otherFiles.foreach { name =>
            val writer = new BufferedWriter(new FileWriter(new File(mergedDir, s"$name.csv")))
            shardDirs.foreach { shard =>
                val file = new File(shard, s"${name}_shard_${shard.getName.split("_").last}.csv")
                if (file.exists()) {
                    val lines = Files.readAllLines(file.toPath).asScala
                    if (lines.nonEmpty) {
                        // Skip header for all but first file
                        val content = if (writer.toString.contains(name)) lines.drop(1) else lines
                        content.foreach { line =>
                            writer.write(line)
                            writer.newLine()
                        }
                    }
                }
            }
            writer.close()
        }
    }
}
