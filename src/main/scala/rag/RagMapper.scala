package rag

import sttp.client3._
import io.circe._, io.circe.parser._
import util.Settings
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io._
import org.slf4j.LoggerFactory

object RagMapper extends Mapper[LongWritable, Text, Text, Text] {
    private val logger = LoggerFactory.getLogger(classOf[rag.RagMapper.type])
    private val backend = HttpURLConnectionBackend()
    private val base = Settings.ollama.host
    private val model = Settings.ollama.embeddingModel

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
        val chunkId = key.toString
        val chunkText = value.toString

        logger.info(s"Processing chunk ID: $chunkId")

        val requestJson =
            s"""
               |{
               |  "model": "$model",
               |  "prompt": "${escapeJson(chunkText)}"
               |}
               |""".stripMargin

        val request = basicRequest
            .body(requestJson)
            .post(uri"$base/api/embeddings")
            .header("Content-Type", "application/json")

        val response = request.send(backend)

        response.body match {
            case Right(jsonStr) =>
                parse(jsonStr).flatMap(_.hcursor.downField("embedding").as[Vector[Float]]) match {
                    case Right(embeddingVec) =>
                        // Store as JSON string or comma-separated string
                        val embeddingStr = embeddingVec.mkString(",")
                        context.write(new Text(chunkId), new Text(embeddingStr))
                        logger.debug(s"Successfully embedded chunk $chunkId")

                    case Left(e) =>
                        logger.error(s"Failed to parse embedding for chunk $chunkId: ${e.getMessage}", e)
                }

            case Left(error) =>
                logger.error(s"Error during embedding request for chunk $chunkId: $error")
        }
    }

    private def escapeJson(str: String): String =
        str.replace("\"", "\\\"").replace("\n", "\\n")
}