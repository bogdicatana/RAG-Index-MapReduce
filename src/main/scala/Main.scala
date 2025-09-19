import util.PdfProcessor

object Main {
    def main(args: Array[String]): Unit = {
        val text = PdfProcessor.readText("/home/bogdan/IdeaProjects/cs441hw1/src/main/resources/MSRCorpus/1083142.1083143.pdf")
        val chunks = PdfProcessor.chunk(text, 20, 1)
        println(chunks)
    }
}