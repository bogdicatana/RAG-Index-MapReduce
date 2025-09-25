package rag

import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, KnnFloatVectorQuery}
import util.Settings

import java.nio.file.Paths

object AskIndex {
    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            println("Usage: AskIndex <index_dir> <question>")
            sys.exit(1)
        }

        val indexDir = args(0)
        val question = args.drop(1).mkString(" ")

        // --- Open Lucene index ---
        val dir = FSDirectory.open(Paths.get(indexDir))
        val reader = DirectoryReader.open(dir)
        val searcher = new IndexSearcher(reader)

        // --- Generate query embedding ---
        val ollama = new Ollama(Settings.ollama.host)
        val queryEmbedding: Array[Float] =
            ollama.embed(Vector(question), Settings.ollama.embeddingModel).head.map(identity)

        // --- Search Lucene index ---
        val query = new KnnFloatVectorQuery("vec", queryEmbedding, reader.numDocs())
        val hits = searcher.search(query, 1)
        val storedFields = searcher.storedFields()

        val retrievedChunks = hits.scoreDocs.map { sd =>
            val d = storedFields.document(sd.doc)
            Option(d.get("text")).getOrElse("")
        }

        println(s"Retrieved ${retrievedChunks.length} chunks")

        // --- Build augmented prompt ---
        val context = retrievedChunks.mkString("\n---\n")
        val fullPrompt =
            s"""Answer the question using only the context below.
               |If the context does not contain the answer, say "I don’t know".
               |
               |Context:
               |$context
               |
               |Question: $question
               |""".stripMargin

        // --- Ask Ollama.chat ---
        val messages = Vector(
            "system" -> "You are a helpful assistant.",
            "user" -> fullPrompt
        )
        val answer = ollama.chat(messages, Settings.ollama.chatModel)
        println(s"\n💬 Answer:\n$answer")


        reader.close()
        dir.close()
    }
}