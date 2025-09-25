package rag

import sttp.client3.*
import sttp.client3.circe.*
import io.circe.*
import io.circe.parser.*
import io.circe.syntax.*
import scala.concurrent.duration.DurationInt


class Ollama(base: String):
    private val backend = HttpClientSyncBackend()
    private val embedUrl = uri"$base/api/embed"
    private val chatUrl = uri"$base/api/chat"

    /** Returns raw embedding vectors from Ollama
     *
     * @param texts
     *   A vector of text strings
     * @param model
     *   Embedding model name (e.g., "mxbai-embed-large")
     * @return
     *   Vector of float arrays (embeddings)
     */
    def embed(texts: Vector[String], model: String): Vector[Array[Float]] =
        val json =
            Json.obj(
                "model" -> Json.fromString(model),
                "input" -> Json.fromValues(texts.map(Json.fromString))
            )

        val req = basicRequest
            .post(embedUrl)
            .body(json.noSpaces)
            .response(asStringAlways)

        val res = req.send(backend)

        parse(res.body) match
            case Right(json) =>
                val cursor = json.hcursor
                cursor
                    .downField("embeddings")
                    .as[Vector[Vector[Float]]]
                    .getOrElse(throw new RuntimeException("Invalid embedding response"))
                    .map(_.toArray)
            case Left(err) =>
                throw new RuntimeException(s"Failed to parse embedding response: $err")

    /** Sends a chat message and returns the response string
     *
     * @param messages
     *   A list of messages (tuples of (role, content))
     * @param model
     *   Chat model name (e.g., "llama3")
     * @return
     *   Response string from the model
     */
    def chat(messages: Vector[(String, String)], model: String): String =
        val json =
            Json.obj(
                "model" -> Json.fromString(model),
                "messages" -> Json.fromValues(
                    messages.map { (role, content) =>
                        Json.obj("role" -> Json.fromString(role), "content" -> Json.fromString(content))
                    }
                ),
                "stream" -> Json.fromBoolean(false)
            )

        val req = basicRequest
            .post(chatUrl)
            .body(json.noSpaces)
            .response(asStringAlways)
            .readTimeout(5.minutes)

        val res = req.send(backend)

        parse(res.body) match
            case Right(json) =>
                json.hcursor.downField("message").downField("content").as[String]
                    .getOrElse(throw new RuntimeException("Missing 'content' in chat response"))
            case Left(err) =>
                throw new RuntimeException(s"Failed to parse chat response: $err")
