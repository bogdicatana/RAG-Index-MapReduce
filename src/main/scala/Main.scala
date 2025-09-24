import rag.Ollama

object Main {
    def main(args: Array[String]): Unit = {
        val ollama = new rag.Ollama(sys.env.getOrElse("OLLAMA_HOST", "http://localhost:11434"))

        val texts = Vector("This is a test.", "This is another test.")
        val embeddings = ollama.embed(texts, "mxbai-embed-large")
        embeddings.foreach(vec => println(vec.mkString(", ")))

        val response = ollama.chat(Vector("user" -> "Tell me a joke."), "llama3.1:8b")
        println(response)
    }
}