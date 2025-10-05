package rag
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, KnnFloatVectorQuery}
import org.apache.lucene.store.FSDirectory
import util.Settings

import java.nio.file.Paths

object InspectIndex {
    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println("Usage: InspectIndex <index_dir>")
            sys.exit(1)
        }

        val dir = FSDirectory.open(Paths.get(args(0)))
        val reader = DirectoryReader.open(dir)
        val searcher = new IndexSearcher(reader)

        println(s"Index contains ${reader.numDocs()} docs")

        // Print first 3 docs
        val storedFields = searcher.storedFields()
        for (i <- 0 until reader.numDocs()) {
            val doc = storedFields.document(i)
            println(s"doc_id=${doc.get("doc_id")}, chunk_id=${doc.get("chunk_id")}")
            println(s"text sample: ${Option(doc.get("text")).getOrElse("").take(100)}...")
        }

        // Example vector query
        val ollama = new Ollama(Settings.ollama.host)
        val queryText = "What is this document about?"
        val queryEmbedding: Array[Float] =
            ollama.embed(Vector(queryText), Settings.ollama.embeddingModel).head.map(identity).toArray

        val query = new KnnFloatVectorQuery("vec", queryEmbedding, 2)
        val hits = searcher.search(query, 2)
        println("Nearest neighbors:")
        val hitFields = searcher.storedFields()
        hits.scoreDocs.foreach { sd =>
            val d = hitFields.document(sd.doc)
            println(s"  -> ${d.get("doc_id")} chunk ${d.get("chunk_id")} score=${sd.score}")
        }

        reader.close()
        dir.close()
    }
}
