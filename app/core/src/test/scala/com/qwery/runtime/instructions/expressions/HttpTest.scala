package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.invocables.Scenario.__KUNGFU_BASE_URL__
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class HttpTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Http].getSimpleName) {

    it("should compile: http get 'http://0.0.0.0:8080/api/shocktrade/contests'") {
      val model = compiler.compile(
        """|http get 'http://0.0.0.0:8080/api/shocktrade/contests'
           |""".stripMargin)
      assert(model == Http(method = "get", url = "http://0.0.0.0:8080/api/shocktrade/contests".v))
    }

    it("should compile: http post 'http://0.0.0.0:8080/api/shocktrade/contests' <~ { name: 'Winter is coming' }") {
      val model = compiler.compile(
        """|http post 'http://0.0.0.0:8080/api/shocktrade/contests' <~ { name: 'Winter is coming' }
           |""".stripMargin)
      assert(model == Http(method = "post", url = "http://0.0.0.0:8080/api/shocktrade/contests".v,
        body = Some(Dictionary("name" -> "Winter is coming".v))))
    }

    it("should decompile: http delete 'http://0.0.0.0:8080/api/shocktrade/contests'") {
      val model = Http(method = "delete", url = "http://0.0.0.0:8080/api/shocktrade/contests".v)
      assert(model.toSQL == """http delete "http://0.0.0.0:8080/api/shocktrade/contests"""")
    }

    it("should decompile: http put 'http://0.0.0.0:8080/api/shocktrade/contests' <~ { name: 'Winter is coming' }") {
      val model = Http(method = "put", url = "http://0.0.0.0:8080/api/shocktrade/contests".v,
        body = Some(Dictionary("name" -> "Winter is coming".v)))
      assert(model.toSQL == """http put "http://0.0.0.0:8080/api/shocktrade/contests" <~ { name: "Winter is coming" }""")
    }

    it("should compile: http post 'http://0.0.0.0:8080/api/u/news' <~ { name: 'ABC News' } ~> ...") {
      val model = compiler.compile(
        """|http post 'http://0.0.0.0:8080/api/u/news'
           |  <~ { name: 'ABC News' }
           |  ~> {
           |      "Connection": "Keep-Alive",
           |      "Content-Encoding": "gzip",
           |      "Content-Type": "text/html; charset=utf-8",
           |      "Date": "Thu, 11 Aug 2016 15:23:13 GMT",
           |      "Keep-Alive": "timeout=5, max=1000",
           |      "Last-Modified": "Mon, 25 Jul 2016 04:32:39 GMT",
           |      "Cookies": ["dandy_cookie=lightbend; tasty_cookie=chocolate"]
           |    }
           |""".stripMargin)
      assert(model == Http(method = "post", url = "http://0.0.0.0:8080/api/u/news".v,
        body = Some(Dictionary("name" -> "ABC News".v)),
        headers = Some(Dictionary(
          "Connection" -> "Keep-Alive".v,
          "Content-Encoding" -> "gzip".v,
          "Content-Type" -> "text/html; charset=utf-8".v,
          "Date" -> "Thu, 11 Aug 2016 15:23:13 GMT".v,
          "Keep-Alive" -> "timeout=5, max=1000".v,
          "Last-Modified" -> "Mon, 25 Jul 2016 04:32:39 GMT".v,
          "Cookies" -> ArrayLiteral("dandy_cookie=lightbend; tasty_cookie=chocolate".v)
        ))))
    }

    it("should return a URL based on a relative path") {
      val (_, _, result: HttpResponse) = QweryVM.executeSQL(Scope()
        .withVariable(name = "host", value = "0.0.0.0")
        .withVariable(name = "port", value = 8233)
        .withVariable(name = "contest_id", value = "40d1857b-474c-4400-8f07-5e04cbacc021")
        .withVariable(name = __KUNGFU_BASE_URL__, code = "http://{{host}}:{{port}}/api/shocktrade/contests?id={{contest_id}}".v, isReadOnly = true),
        """|http path "users"
           |""".stripMargin)
      assert(result.body == "http://0.0.0.0:8233/api/shocktrade/users")
    }

  }

}
