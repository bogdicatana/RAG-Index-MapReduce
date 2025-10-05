package rag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io.Source
import java.io.PrintWriter
import scala.util.Using

object StatsMerger {

    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            System.err.println("Usage: StatsMerger <hdfsOutputDir> <mergedStatsDir>")
            System.exit(1)
        }

        val outputDir = new Path(args(0))
        val mergedDir = new Path(args(1))
        val conf = new Configuration()
        val fs = FileSystem.get(conf)

        fs.mkdirs(mergedDir)

        mergeVocab(fs, outputDir, mergedDir)
        mergeCSV(fs, outputDir, mergedDir, "neighbors")
        mergeCSV(fs, outputDir, mergedDir, "similarities")
        mergeCSV(fs, outputDir, mergedDir, "analogies")

        println(s"✅ Merged stats written to: $mergedDir")
    }

    // --- 1️⃣ Merge vocabulary by summing frequencies ---
    private def mergeVocab(fs: FileSystem, baseDir: Path, mergedDir: Path): Unit = {
        val shardFiles = fs.listStatus(baseDir)
            .filter(_.getPath.getName.startsWith("stats_shard_"))
            .flatMap { shardDir =>
                val shardPath = shardDir.getPath
                fs.listStatus(shardPath)
                    .filter(_.getPath.getName.startsWith("vocab"))
                    .map(_.getPath)
            }

        if (shardFiles.isEmpty) {
            println("No vocab files found, skipping vocab merge.")
            return
        }

        println(s"Merging ${shardFiles.length} vocab files...")

        // Aggregate counts
        val mergedCounts = shardFiles.foldLeft(Map.empty[String, Long]) { (acc, filePath) =>
            Using.resource(Source.fromInputStream(fs.open(filePath))) { src =>
                src.getLines().drop(1).foldLeft(acc) { (map, line) =>
                    val parts = line.split(",", 2)
                    if (parts.length == 2) {
                        val word = parts(0).trim
                        val freq = parts(1).trim.toLongOption.getOrElse(0L)
                        map.updated(word, map.getOrElse(word, 0L) + freq)
                    } else map
                }
            }
        }

        // Sort by descending frequency
        val sorted = mergedCounts.toSeq.sortBy(-_._2)

        val mergedPath = new Path(mergedDir, "vocab_merged.csv")
        Using.resource(new PrintWriter(fs.create(mergedPath, true))) { out =>
            out.println("word,freq")
            sorted.foreach { case (word, freq) => out.println(s"$word,$freq") }
        }

        println(s"✅ Merged vocab: ${sorted.size} unique tokens written to $mergedPath")
    }

    // --- 2️⃣ Merge other CSVs by concatenation ---
    private def mergeCSV(fs: FileSystem, baseDir: Path, mergedDir: Path, prefix: String): Unit = {
        val shardFiles = fs.listStatus(baseDir)
            .filter(_.getPath.getName.startsWith("stats_shard_"))
            .flatMap { shardDir =>
                val shardPath = shardDir.getPath
                fs.listStatus(shardPath)
                    .filter(_.getPath.getName.startsWith(prefix))
                    .map(_.getPath)
            }

        if (shardFiles.isEmpty) {
            println(s"No $prefix files found, skipping.")
            return
        }

        val mergedPath = new Path(mergedDir, s"${prefix}_merged.csv")
        Using.resource(new PrintWriter(fs.create(mergedPath, true))) { out =>
            var wroteHeader = false

            shardFiles.foreach { f =>
                Using.resource(Source.fromInputStream(fs.open(f))) { src =>
                    src.getLines().foreach { line =>
                        if (!wroteHeader && line.contains(",")) {
                            out.println(line)
                            wroteHeader = true
                        } else if (!line.startsWith("word") && !line.startsWith("token") && !line.startsWith("a,")) {
                            out.println(line)
                        }
                    }
                }
            }
        }

        println(s"Merged ${shardFiles.length} $prefix files -> $mergedPath")
    }
}
