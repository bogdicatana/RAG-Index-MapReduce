package util

import com.typesafe.config.ConfigFactory

object Settings {
    private val config = ConfigFactory.load()

    // Encapsulate all settings in a case class for clarity
    final case class OllamaSettings(host: String, embeddingModel: String, chatModel: String)

    final case class ChunkerSettings(maxChars: Int, overlap: Int)

    val ollama = OllamaSettings(
        config.getString("rag-builder.ollama.host"),
        config.getString("rag-builder.ollama.embedding-model"),
        config.getString("rag-builder.ollama.chat-model")
    )

    val chunker = ChunkerSettings(
        config.getInt("rag-builder.chunker.max-chars"),
        config.getInt("rag-builder.chunker.overlap")
    )
}
