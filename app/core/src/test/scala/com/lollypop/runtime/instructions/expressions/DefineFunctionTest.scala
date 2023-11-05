package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.Operation.RichOperation
import com.lollypop.language.models._
import com.lollypop.language.{TokenIterator, TokenStream}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.conditions.LTE
import com.lollypop.runtime.instructions.functions.{AnonymousFunction, NamedFunction}
import com.lollypop.runtime.instructions.invocables.{DefineFunction, IF, Import}
import com.lollypop.runtime.instructions.operators.{Minus, Plus, Times}
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class DefineFunctionTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[AnonymousFunction].getSimpleName) {

    it("should decompile to SQL") {
      val model =
        AnonymousFunction(
          params = List(Column("name String")),
          code = Plus(Literal("Hello "), "name".f)
        )
      assert(model.toSQL ===
        """|(name: String) => "Hello " + name
           |""".stripMargin.trim)
    }

    it("should compile: (n: Int) => n * n") {
      val ts = TokenStream(TokenIterator("(n: Int) => n * n"))
      assert(compiler.nextExpression(ts) contains AnonymousFunction(
        params = List(Parameter("n Int")),
        code = Times("n ".f, "n".f)
      ))
    }

    it("should compile: ((n: Int) => { n * n })(5)") {
      assert(compiler.compile("((n: Int) => { n * n })(5)") == ApplyTo(AnonymousFunction(
        params = List(Parameter("n Int")),
        code = CodeBlock(Times("n ".f, "n".f))
      ), 5.v))
    }

    it("should execute an anonymous function") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|((n: Int) => n * n)(5)
           |""".stripMargin)
      assert(result == 25)
    }

    it("should execute an anonymous queryable") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|((total: Int) => {
           |    declare table myQuotes(id RowNumber, symbol: String(4), exchange: String(6), lastSale Float, lastSaleTime: DateTime);
           |    [1 to total].foreach((cnt: Int) => {
           |        insert into @myQuotes (lastSaleTime, lastSale, exchange, symbol)
           |        select lastSaleTime: DateTime() - Interval(Random.nextInt(25000000) + ' milli'),
           |               lastSale: scaleTo(150 * Random.nextDouble(0.99), 4),
           |               exchange: ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB'][Random.nextInt(4)],
           |               symbol: Random.nextString(['A' to 'Z'], 4)
           |    })
           |    val summary = select exchange, total: count(*) from @myQuotes group by exchange
           |    summary.show(5)
           |    myQuotes
           |})(8)
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.getLength == 8)
      assert(device.columns.map(_.name) == List("id", "symbol", "exchange", "lastSale", "lastSaleTime"))
    }

    it("should support function closures") {
      val (scopeA, _, resultA) = LollypopVM.executeSQL(Scope(),
        """|val factory = () => {
           |    var n: Int = 0
           |    () => { n += 1 n }
           |}
           |
           |val counter = factory()
           |counter()
           |counter()
           |counter()
           |""".stripMargin)
      var myScope: Option[Scope] = Some(scopeA)
      var level = 1
      do {
        myScope.foreach(_.show(s"level $level").foreach(logger.info))
        myScope = myScope.flatMap(_.getSuperScope)
        level += 1
      } while (myScope.exists(_.getSuperScope.nonEmpty))
      assert(resultA == 3)
    }

  }

  describe(classOf[NamedFunction].getSimpleName) {

    it("should produce the appropriate model") {
      val model = compiler.compile(
        """|def factorial(n: Int): Int := if(n <= 1) 1 else n * factorial(n - 1)
           |""".stripMargin)
      assert(model == DefineFunction(NamedFunction(
        name = "factorial",
        params = List(Column(name = "n", `type` = "Int".ct)),
        code = IF(LTE("n".f, 1.v), 1.v, Some(Times("n".f, NamedFunctionCall("factorial", List(Minus("n".f, 1.v)))))),
        returnType_? = Some("Int".ct)
      )))
    }

    it("should produce the appropriate model without a return type") {
      val model = compiler.compile(
        """|def factorial(n: Int) := if(n <= 1) 1 else n * factorial(n - 1)
           |""".stripMargin)
      assert(model == DefineFunction(NamedFunction(
        name = "factorial",
        params = List(Column(name = "n", `type` = "Int".ct)),
        code = IF(LTE("n".f, 1.v), 1.v, Some(Times("n".f, NamedFunctionCall("factorial", List(Minus("n".f, 1.v)))))),
        returnType_? = None
      )))
    }

    it("should produce the appropriate model without parameter or return types") {
      val model = compiler.compile(
        """|def pythagoras(a, b) := {
           |    import "java.lang.Math"
           |    Math.sqrt((a * a) + (b * b))
           |}
           |""".stripMargin)
      assert(model == DefineFunction(NamedFunction(
        name = "pythagoras",
        params = List(Column(name = "a", `type` = "Any".ct), Column(name = "b", `type` = "Any".ct)),
        code = CodeBlock(
          Import("java.lang.Math".v),
          Infix("Math".f, "sqrt".fx(("a".f * "a".f) + ("b".f * "b".f)))
        ),
        returnType_? = None
      )))
    }

    it("should render itself as SQL") {
      val model = compiler.compile(
        """|def factorial(n: Int): Int := if(n <= 1) 1 else n * factorial(n - 1)
           |""".stripMargin)
      assert(model.toSQL == "def factorial(n: Int): Int := if(n <= 1) 1 else n * factorial(n - 1)")
    }

    it("should define an invocable function") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|{
           |def factorial(n: Int): Int := if(n <= 1) 1 else n * factorial(n - 1)
           |factorial(5)
           |}
           |""".stripMargin)
      assert(result == 120)
    }

    it("should define an invocable function without a return type") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|{
           |def factorial(n: Int) := if(n <= 1) 1 else n * factorial(n - 1)
           |factorial(6)
           |}
           |""".stripMargin)
      assert(result == 720)
    }

  }

}
