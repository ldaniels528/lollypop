package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.invocables.Scenario.__KUNGFU_BASE_URL__
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class WWWTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[WWW].getSimpleName) {

    it("should compile: www get 'http://0.0.0.0:8080/api/shocktrade/contests'") {
      val model = compiler.compile(
        """|www get 'http://0.0.0.0:8080/api/shocktrade/contests'
           |""".stripMargin)
      assert(model == WWW(method = "get", url = "http://0.0.0.0:8080/api/shocktrade/contests".v))
    }

    it("should compile: www post 'http://0.0.0.0:8080/api/shocktrade/contests' <~ { name: 'Winter is coming' }") {
      val model = compiler.compile(
        """|www post 'http://0.0.0.0:8080/api/shocktrade/contests' <~ { name: 'Winter is coming' }
           |""".stripMargin)
      assert(model == WWW(method = "post", url = "http://0.0.0.0:8080/api/shocktrade/contests".v,
        body = Some(Dictionary("name" -> "Winter is coming".v))))
    }

    it("should decompile: www delete 'http://0.0.0.0:8080/api/shocktrade/contests'") {
      val model = WWW(method = "delete", url = "http://0.0.0.0:8080/api/shocktrade/contests".v)
      assert(model.toSQL == """www delete "http://0.0.0.0:8080/api/shocktrade/contests"""")
    }

    it("should decompile: www put 'http://0.0.0.0:8080/api/shocktrade/contests' <~ { name: 'Winter is coming' }") {
      val model = WWW(method = "put", url = "http://0.0.0.0:8080/api/shocktrade/contests".v,
        body = Some(Dictionary("name" -> "Winter is coming".v)))
      assert(model.toSQL == """www put "http://0.0.0.0:8080/api/shocktrade/contests" <~ { name: "Winter is coming" }""")
    }

    it("should compile: www post 'http://0.0.0.0:8080/api/u/news' <~ { name: 'ABC News' } ~> ...") {
      val model = compiler.compile(
        """|www post 'http://0.0.0.0:8080/api/u/news'
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
      assert(model == WWW(method = "post", url = "http://0.0.0.0:8080/api/u/news".v,
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
      val (_, _, result: HttpResponse) = LollypopVM.executeSQL(Scope()
        .withVariable(name = "host", value = "0.0.0.0")
        .withVariable(name = "port", value = 8233)
        .withVariable(name = "contest_id", value = "40d1857b-474c-4400-8f07-5e04cbacc021")
        .withVariable(name = __KUNGFU_BASE_URL__, code = "http://{{host}}:{{port}}/api/shocktrade/contests?id={{contest_id}}".v, isReadOnly = true),
        """|www path "users"
           |""".stripMargin)
      assert(result.body == "http://0.0.0.0:8233/api/shocktrade/users")
    }

  }

}
