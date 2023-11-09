package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_ASYNC_REACTIVE, PARADIGM_REACTIVE}
import com.lollypop.language.models.{Atom, Expression}
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream, dieIllegalType}
import com.lollypop.runtime.devices.QMap
import com.lollypop.runtime.instructions.expressions.Http.wget
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.invocables.Scenario.__KUNGFU_BASE_URL__
import com.lollypop.runtime.{Scope, safeCast}
import com.lollypop.util.JSONSupport.JSONProductConversion
import com.lollypop.util.IOTools
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper._
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.io.IOCost

import java.io.File
import java.net.{HttpURLConnection, URI}
import java.util.UUID
import scala.concurrent.duration.{Duration, DurationInt}

/**
 * HTTP/REST Client
 * @example http get "http://localhost:8097/user/get"
 * @example {{{
 *  http post 'http://0.0.0.0:{{port}}/api/demo/subscriptions'
 *    <~ { name: 'ABC News', startTime: DateTime('2022-09-04T23:36:46.862Z') }
 *    ~> {
 *        "Connection": "Keep-Alive",
 *        "Content-Encoding": "gzip",
 *        "Content-Type": "text/html; charset=utf-8",
 *        "Date": "Thu, 11 Aug 2016 15:23:13 GMT",
 *        "Keep-Alive": "timeout=5, max=1000",
 *        "Last-Modified": "Mon, 25 Jul 2016 04:32:39 GMT",
 *        "Cookies": ["dandy_cookie=lightbend; tasty_cookie=chocolate"]
 *      }
 * }}}
 */
case class Http(method: Atom, url: Expression, body: Option[Expression] = None, headers: Option[Expression] = None)
  extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, HttpResponse) = {

    def getAbsoluteURL: String = {
      (for {
        path <- url.asString
        url <- (scope.resolve("url") ?? scope.resolve(__KUNGFU_BASE_URL__)).flatMap(safeCast[String])
      } yield new URI(url).resolve(path).toString).orNull
    }

    val result = method.name.toLowerCase() match {
      case "path" => HttpResponse(body = getAbsoluteURL, message = null, statusCode = 200, responseID = UUID.randomUUID())
      case "uri" => HttpResponse(body = getAbsoluteURL, message = null, statusCode = 200, responseID = UUID.randomUUID())
      case _method =>
        wget(
          url = url.asString || dieUrlIsNull(),
          body = body.flatMap(_.asDictionary).map(_.toJsValue.toString()),
          headers = headers.flatMap(_.asDictionary),
          method = Some(_method))
    }
    (scope, IOCost.empty, result)
  }

  override def toSQL: String = {
    ("http" :: method.toSQL :: url.toSQL :: body.toList.flatMap(v => List("<~", v.toSQL)) ::: headers.toList.flatMap(h => List("~>", h.toSQL))).mkString(" ")
  }

}

object Http extends ExpressionParser {
  val template: String =
    """|http %a:method %e:url ?<~ +?%e:body ?~> +?%e:headers
       |""".stripMargin

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "http",
    category = CATEGORY_ASYNC_REACTIVE,
    paradigm = PARADIGM_REACTIVE,
    syntax = template,
    description = "Lollypop-native HTTP client",
    example =
      """|http get('https://example.com/')
         |""".stripMargin
  ), HelpDoc(
    name = "http",
    category = CATEGORY_ASYNC_REACTIVE,
    paradigm = PARADIGM_REACTIVE,
    syntax = template,
    description = "Returns a URL based on a relative path.",
    isExperimental = true,
    example =
      """|http path('users')
         |""".stripMargin
  ), HelpDoc(
    name = "http",
    category = CATEGORY_ASYNC_REACTIVE,
    paradigm = PARADIGM_REACTIVE,
    syntax = template,
    description = "Returns a URI based on a relative path.",
    isExperimental = true,
    example =
      """|http uri('users')
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Http] = {
    val params = SQLTemplateParams(ts, template)
    Option(Http(
      method = params.atoms("method"),
      url = params.expressions("url"),
      body = params.expressions.get("body"),
      headers = params.expressions.get("headers")))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "http"

  private def wget(url: String,
                   body: Option[Any] = None,
                   connectionTimeout: Duration = 5.second,
                   headers: Option[QMap[String, Any]] = None,
                   method: Option[String] = None,
                   readTimeout: Duration = 15.second,
                   expectResponse: Boolean = true): HttpResponse = {
    import spray.json._
    val defaultHeaders = Map[String, Any](
      "Accept" -> "application/json",
      "Content-Type" -> "application/json; charset=UTF-8",
      "User-Agent" -> "Mozilla/5.0"
    )
    new URI(url).toURL.openConnection() match {
      case conn: HttpURLConnection =>
        try {
          conn.setConnectTimeout(connectionTimeout.toMillis.toInt)
          conn.setReadTimeout(readTimeout.toMillis.toInt)
          conn.setRequestMethod(method.map(_.toUpperCase) || "GET")
          (defaultHeaders ++ (headers || Map.empty)).foreach({ case (name, value) =>
            conn.setRequestProperty(name, value.render)
          })
          conn.setDoOutput(body.nonEmpty)
          if (expectResponse) conn.setDoInput(expectResponse)
          body foreach {
            case f: File => IOTools.transfer(f, conn)
            case j: JsValue => conn.getOutputStream.use(_.write(j.toString().getBytes))
            case s: String => conn.getOutputStream.use(_.write(s.getBytes))
            case x => dieIllegalType(x)
          }
          HttpResponse(conn, expectResponse)
        } catch {
          case e: Exception =>
            HttpResponse(body = "", statusCode = 500, message = e.getMessage)
        }
      case conn =>
        throw new IllegalArgumentException(s"Invalid connection type $conn [$method|$url]")
    }
  }

}