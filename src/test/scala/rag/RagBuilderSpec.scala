package rag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.pdfbox.pdmodel.font.{PDType1Font, Standard14Fonts}
import org.apache.pdfbox.pdmodel.{PDDocument, PDPage, PDPageContentStream}
import org.scalatest.funsuite.AnyFunSuite
import util.*

import java.io.File
import scala.language.reflectiveCalls
import util.Settings


class RagBuilderSpec extends AnyFunSuite {

    // ========== 1. PdfProcessor Tests ==========

    test("PdfProcessor.readText extracts text correctly") {
        val tmp = File.createTempFile("test_pdf", ".pdf")
        val doc = new PDDocument()
        val page = new PDPage()
        doc.addPage(page)

        val content = new PDPageContentStream(doc, page)
        content.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12f)
        content.beginText()
        content.newLineAtOffset(100, 700)
        content.showText("Hello PDF!")
        content.endText()
        content.close()

        doc.save(tmp)
        doc.close()

        val text = PdfProcessor.readText(tmp.getAbsolutePath)
        assert(text.contains("Hello PDF!"))
    }

    test("PdfProcessor.chunk splits text with overlap") {
        val text = "one two three four five six seven eight nine ten"
        val chunks = PdfProcessor.chunk(text, chunkSize = 5, overlap = 2)
        assert(chunks.nonEmpty)
        assert(chunks.head.contains("one two"))
        assert(chunks.last.contains("nine"))
    }

    // ========== 2. RagMapper Tests ==========

    test("RagMapper + ShardReducer run in local mode") {

        // --- Setup local Hadoop job ---
        val conf = new Configuration(false)
        conf.set("mapreduce.framework.name", "local")
        conf.set("fs.defaultFS", "file:///")

        val job = Job.getInstance(conf, "rag-index-builder-test")
        job.setJarByClass(classOf[RagMapper])
        job.setMapperClass(classOf[RagMapper])
        job.setReducerClass(classOf[ShardReducer])

        job.setMapOutputKeyClass(classOf[IntWritable])
        job.setMapOutputValueClass(classOf[Text])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])
        job.setNumReduceTasks(1)

        FileInputFormat.addInputPath(job, new Path(Settings.inputPDFS))

        // Use a unique output directory per test run
        val outputDir = new File(System.getProperty("java.io.tmpdir"), s"rag-output-${System.currentTimeMillis()}")
        FileOutputFormat.setOutputPath(job, new Path(outputDir.getAbsolutePath))

        // --- Run the job ---
        val success = job.waitForCompletion(true)
        assert(success, "MapReduce job failed")

        // --- Check that output files exist ---
        val outputFiles = outputDir.listFiles().toSeq
        assert(outputFiles.exists(_.getName.contains("_SUCCESS")), "Job did not write _SUCCESS file")
        val statsPath = new File(outputDir, "stats_shard_0")
        val vocabCsv = new File(statsPath, "vocab_shard_0.csv")
        assert(vocabCsv.exists(), "Vocab CSV should exist")
        val neighborsCsv = new File(statsPath, "neighbors_shard_0.csv")
        val simCsv = new File(statsPath, "similarities_shard_0.csv")
        val anaCsv = new File(statsPath, "analogies_shard_0.csv")

        Seq(neighborsCsv, simCsv, anaCsv).foreach(f => assert(f.exists(), s"${f.getName} should exist"))
    }
    // Ollama client integration tests
    test("Ollama.embed returns embeddings of correct shape") {
        val client = new Ollama(Settings.ollama.host)
        val texts = Vector("Hello world", "Another test sentence")

        val embeddings = client.embed(texts, Settings.ollama.embeddingModel)
        assert(embeddings.length == texts.length, "Number of embeddings should match input texts")
        embeddings.foreach { vec =>
            assert(vec.nonEmpty, "Embedding vector should not be empty")
        }
    }

    test("Ollama.chat returns a non-empty response") {
        val client = new Ollama(Settings.ollama.host)
        val messages = Vector(
            ("user", "Hello!"),
            ("user", "Can you summarize this text?")
        )

        val response = client.chat(messages, Settings.ollama.chatModel)
        assert(response.nonEmpty, "Chat response should not be empty")
        assert(response.isInstanceOf[String], "Chat response should be a string")
    }

}
