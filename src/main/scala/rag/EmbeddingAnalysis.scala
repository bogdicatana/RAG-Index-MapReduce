package rag

import util.{PdfProcessor, Settings}
import java.io.{File, PrintWriter}
import scala.util.Using

object EmbeddingAnalysis {

    // --- Helpers ---
    private def tokenize(text: String): Seq[String] =
        text.toLowerCase.split("\\W+").filter(_.nonEmpty)

    private def cosineSimilarity(a: Array[Float], b: Array[Float]): Double = {
        val dot = a.zip(b).map { case (x, y) => x * y }.sum
        val normA = math.sqrt(a.map(x => x * x).sum)
        val normB = math.sqrt(b.map(x => x * x).sum)
        dot / (normA * normB)
    }

    // --- Main ---
    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println("Usage: EmbeddingAnalysis <pdf_dir>")
            sys.exit(1)
        }

        val pdfDir = new File(args(0))
        val pdfFiles = pdfDir.listFiles().filter(_.getName.endsWith(".pdf"))

        println(s"Processing ${pdfFiles.length} PDFs...")

        // 1. Extract corpus text
        val corpus = pdfFiles.map(f => PdfProcessor.readText(f.getAbsolutePath)).mkString("\n")
        val tokens = tokenize(corpus)

        // 2. Vocabulary with frequency
        val freq = tokens.groupBy(identity).view.mapValues(_.size).toMap
        val vocab = freq.toSeq.sortBy(-_._2) // sort by frequency descending
        val vocabWithIds = vocab.zipWithIndex.map { case ((w, c), id) => (id, w, c) }

        // Write vocab.csv
        Using.resource(new PrintWriter("vocab.csv")) { out =>
            out.println("id,word,freq")
            vocabWithIds.foreach { case (id, word, count) =>
                out.println(s"$id,$word,$count")
            }
        }
        println(s"✅ Wrote vocab.csv (size=${vocabWithIds.size})")

        // 3. Generate embeddings for top N words
        val topN = 200
        val words = vocabWithIds.take(topN).map(_._2)
        val ollama = new Ollama(Settings.ollama.host)
        println(s"Embedding $topN words...")
        val embeddings: Map[String, Array[Float]] =
            words.map(w => w -> ollama.embed(Vector(w), Settings.ollama.embeddingModel).head.map(identity)).toMap

        // 4. Nearest neighbors
        Using.resource(new PrintWriter("neighbors.csv")) { out =>
            out.println("word,neighbor,similarity")
            for ((word, vec) <- embeddings) {
                val neighbors = embeddings.filterKeys(_ != word)
                    .map { case (w2, v2) => (w2, cosineSimilarity(vec, v2)) }
                    .toSeq.sortBy(-_._2).take(5)
                neighbors.foreach { case (n, sim) =>
                    out.println(s"$word,$n,$sim")
                }
            }
        }
        println("✅ Wrote neighbors.csv")

        // 5. Word similarity evaluation
        val simPairs = Seq(
            ("cat", "dog"),
            ("car", "automobile"),
            ("king", "queen"),
            ("cat", "car")
        )
        Using.resource(new PrintWriter("similarities.csv")) { out =>
            out.println("word1,word2,similarity")
            simPairs.foreach { case (w1, w2) =>
                for {
                    v1 <- embeddings.get(w1)
                    v2 <- embeddings.get(w2)
                } {
                    val sim = cosineSimilarity(v1, v2)
                    out.println(s"$w1,$w2,$sim")
                }
            }
        }
        println("✅ Wrote similarities.csv")

        // 6. Word analogy evaluation
        val analogies = Seq(
            ("king", "man", "woman", "queen"),
            ("paris", "france", "italy", "rome")
        )
        Using.resource(new PrintWriter("analogies.csv")) { out =>
            out.println("a,b,c,expected,predicted")
            analogies.foreach { case (a, b, c, expected) =>
                for {
                    va <- embeddings.get(a)
                    vb <- embeddings.get(b)
                    vc <- embeddings.get(c)
                } {
                    val analogyVec = va.zip(vb).map { case (xa, xb) => xa - xb }
                        .zip(vc).map { case (x, xc) => x + xc }

                    val predicted = embeddings
                        .filterKeys(w => w != a && w != b && w != c)
                        .maxBy { case (_, v) => cosineSimilarity(analogyVec, v) }._1

                    out.println(s"$a,$b,$c,$expected,$predicted")
                }
            }
        }
        println("✅ Wrote analogies.csv")
    }
}
