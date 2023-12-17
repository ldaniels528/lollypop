package com.lollypop.runtime.instructions.expressions

import com.lollypop.runtime._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.ByteArrayOutputStream

import java.net.HttpURLConnection
import java.util.UUID
import scala.util.Try

/**
 * Represents an HTTP response
 * @param body       the response body as an [[Any object]] or byte array
 * @param message    the optional response status/error message
 * @param statusCode the HTTP status code (e.g. 200 for "HTTP/200 OK")
 */
case class HttpResponse(body: Any,
                        message: String,
                        statusCode: Int,
                        responseID: UUID = UUID.randomUUID())

/**
 * HTTP Response Companion
 */
object HttpResponse {
  def apply(conn: HttpURLConnection, expectResponse: Boolean): HttpResponse = {
    import spray.json._
    try {
      // get the body as a byte array
      val bytes: Array[Byte] = if (!expectResponse) Array.empty else {
        conn.getInputStream use { in =>
          val available = in.available()
          val out = new ByteArrayOutputStream(available)
          val copied = IOUtils.copy(in, out)
          val content = out.toByteArray
          assert(available <= copied, s"Copy error: $available > $copied")
          content
        }
      }

      // attempt to provide the best body format (JSON, text or bytes)
      new HttpResponse(
        body = {
          val text = new String(bytes)
          Try(text.parseJson.unwrapJSON).getOrElse(if (text.forall(c => c >= '\t' & c <= 127)) text else bytes)
        },
        message = conn.getResponseMessage,
        statusCode = conn.getResponseCode)
    } catch {
      case e: Exception =>
        new HttpResponse(
          body = JsNull,
          message = e.getMessage,
          statusCode = 500)
    }
  }
}