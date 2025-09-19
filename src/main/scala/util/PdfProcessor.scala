package util

import org.apache.pdfbox.Loader
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

import java.io.File

object PdfProcessor {
    def readText(path: String): String = {
        val doc: PDDocument = Loader.loadPDF(new File(path))
        try{
            new PDFTextStripper().getText(doc)
        } finally{
            doc.close()
        }
    }

    def chunk(text: String, chunkSize: Int, overlap: Int): Vector[String] = {
        require(overlap < chunkSize, "Overlap must be smaller than chunk size")

        val words = text.split("\\s+").toVector
        val step = chunkSize - overlap

        words.sliding(chunkSize, step)
            .map(_.mkString(" "))
            .toVector
    }
}
