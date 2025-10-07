package util

import com.typesafe.config.ConfigFactory
import scala.jdk.CollectionConverters.*

object Settings {
    private val config = ConfigFactory.load()

    // Encapsulate all settings in a case class for clarity
    final case class OllamaSettings(host: String, embeddingModel: String, chatModel: String, query: String, timeout: Int, topResults: Int)

    final case class ChunkerSettings(maxChars: Int, overlap: Int)

    final case class OutputSettings(outputDir: String, mergeDir: String)

    val ollama = OllamaSettings(
        config.getString("rag-builder.ollama.host"),
        config.getString("rag-builder.ollama.embedding-model"),
        config.getString("rag-builder.ollama.chat-model"),
        config.getString("rag-builder.ollama.query"),
        config.getInt("rag-builder.ollama.timeout"),
        config.getInt("rag-builder.ollama.topResults")
    )

    val chunker = ChunkerSettings(
        config.getInt("rag-builder.chunker.max-chars"),
        config.getInt("rag-builder.chunker.overlap")
    )
    
    lazy val inputPDFS : String = {
        config.getString("rag-builder.input.pdfs")
    }
    
    lazy val numReduceJobs: Int = {
        config.getInt("rag-builder.mapReduce.numReduceJobs")
    }

    lazy val output = OutputSettings(
        config.getString("rag-builder.output.outputDir"),
        config.getString("rag-builder.output.mergeDir")
    )

    // --- 🧠 Word Relation Settings ---
    lazy val similarityPairs: Seq[(String, String)] = {
        config.getList("rag-builder.word-relations.similarities")
            .asScala
            .map { entry =>
                val lst = entry.unwrapped().asInstanceOf[java.util.List[String]].asScala
                (lst.head, lst(1))
            }.toSeq
    }

    lazy val analogyTriplets: Seq[(String, String, String)] = {
        config.getList("rag-builder.word-relations.analogies")
            .asScala
            .map { entry =>
                val lst = entry.unwrapped().asInstanceOf[java.util.List[String]].asScala
                (lst.head, lst(1), lst(2))
            }.toSeq
    }
}
