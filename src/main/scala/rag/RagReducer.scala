package rag

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Reducer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index._
import org.apache.lucene.store._
import org.apache.lucene.util.Version
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class RagReducer extends Reducer[Text, Text, NullWritable, NullWritable] {

    private val logger = LoggerFactory.getLogger(classOf[rag.RagReducer])
    private var writer: IndexWriter = _

    override def setup(context: Reducer[Text, Text, NullWritable, NullWritable]#Context): Unit = {
        val indexPath = Paths.get("lucene_index")  // You can make this configurable or per-task
        val directory: Directory = FSDirectory.open(indexPath)
        val analyzer = new StandardAnalyzer()
        val config = new IndexWriterConfig(analyzer)
        writer = new IndexWriter(directory, config)

        logger.info("Lucene IndexWriter initialized")
    }

    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, NullWritable, NullWritable]#Context): Unit = {
        val chunkId = key.toString
        val valueList = values.asScala.toList

        // Each value is the embedding as CSV. Assume original text is retrievable or embedded.
        valueList.foreach { embeddingStr =>
            try {
                // Here we index chunkId and embedding vector as stored fields
                // You may want to attach the original text here too

                val doc = new Document()
                doc.add(new StringField("chunkId", chunkId, Field.Store.YES))
                doc.add(new StoredField("embedding", embeddingStr.toString))

                // Optional: if you also have the original text, add it for full-text search
                val originalText = getChunkText(chunkId)  // Stub function - replace as needed
                doc.add(new TextField("content", originalText, Field.Store.YES))

                writer.addDocument(doc)
                logger.info(s"Indexed chunk: $chunkId")
            } catch {
                case e: Exception =>
                    logger.error(s"Failed to index chunk $chunkId: ${e.getMessage}", e)
            }
        }
    }

    override def cleanup(context: Reducer[Text, Text, NullWritable, NullWritable]#Context): Unit = {
        writer.close()
        logger.info("Lucene index written and writer closed.")
    }

    // 🧪 Placeholder - you'll need to wire this to your actual chunk text store
    private def getChunkText(chunkId: String): String = {
        // Load from HDFS, local file, or in-memory map
        // For now, return dummy text
        s"This is a placeholder content for chunk $chunkId"
    }
}
