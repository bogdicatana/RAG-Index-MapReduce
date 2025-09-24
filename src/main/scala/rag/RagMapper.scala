package rag

import util.{PdfProcessor, Settings}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.*
import org.slf4j.LoggerFactory

import java.nio.file.Paths

object RagMapper extends Mapper[LongWritable, Text, IntWritable, Text] {
    private val logger = LoggerFactory.getLogger(classOf[rag.RagMapper.type])
    private val model = Settings.ollama.embeddingModel
    private val client = new Ollama(Settings.ollama.host)

    
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {
        val path = Paths.get(value.toString)
        val docId = path.getFileName.toString

        val text = PdfProcessor.readText(path.toString)
        val chunks = PdfProcessor.chunk(text, Settings.chunker.maxChars, Settings.chunker.overlap)
        val embeddings = client.embed(chunks, model).map(Vectors.l2) // assuming Vectors.l2 converts float[] to normalized vector

        // Calculate shard ID (partition based on docId hash)
        val numReducers = context.getNumReduceTasks
        val shard = if (numReducers > 0) Math.abs(docId.hashCode) % numReducers else 0

        // Emit each chunk with embedding as JSON string
        chunks.zip(embeddings).zipWithIndex.foreach { case ((chunk, vec), chunkId) =>
            val json = s"""{"doc_id":"$docId","chunk_id":$chunkId,"text":${encodeJson(chunk)},"vec":[${vec.mkString(",")}]}"""
            context.write(new IntWritable(shard), new Text(json))
        }
    }

    // Helper to escape string for JSON
    private def encodeJson(s: String): String =
        "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}

// Helper object for vector normalization (example)
object Vectors {
    def l2(vec: Array[Float]): Array[Float] = {
        val norm = math.sqrt(vec.map(x => x * x).sum).toFloat
        if (norm == 0f) vec else vec.map(_ / norm)
    }
}
