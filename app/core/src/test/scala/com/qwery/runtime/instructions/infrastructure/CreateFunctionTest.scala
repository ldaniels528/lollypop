package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Operation.RichOperation
import com.qwery.language.models.{@@, TypicalFunction}
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class CreateFunctionTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[CreateFunction].getSimpleName) {

    it("should support create function") {
      val results = compiler.compile(
        """|create function if not exists calc_add(a Int, b Int) := @a + @b
           |""".stripMargin)
      assert(results == CreateFunction(ref = DatabaseObjectRef("calc_add"),
        TypicalFunction(
          params = Seq("a Int".c, "b Int".c),
          code = @@("a") + @@("b")
        ), ifNotExists = true))
    }

    it("should support decompiling create function") {
      verify(
        """|create function if not exists calc_add(a Int, b Int) := @a + @b
           |""".stripMargin)
    }

    it("should execute create function") {
      val (scope0, _, results0) = QweryVM.searchSQL(Scope(),
        """|namespace 'samples.functions'
           |drop if exists calc_add
           |create function calc_add(a Int, b Int) := a + b
           |select calc_add(7, 5)
           |""".stripMargin)
      results0.tabulate() foreach logger.info
      assert(results0.toMapGraph == List(Map("calc_add" -> 12.0)))

      // call with fully qualified name
      val (scope1, _, results1) = QweryVM.searchSQL(scope0,
        """|select value: `samples.functions.calc_add`(11, 8)
           |""".stripMargin)
      results1.tabulate() foreach logger.info
      assert(results1.toMapGraph == List(Map("value" -> 19.0)))

      // must exist in __objects__
      val (scope2, _, results2) = QweryVM.searchSQL(scope1,
        """|select database, schema, name, `type`
           |from (OS.getDatabaseObjects())
           |where qname is 'samples.functions.calc_add'
           |""".stripMargin
      )
      results2.tabulate() foreach logger.info
      assert(results2.toMapGraph == List(
        Map("name" -> "calc_add", "schema" -> "functions", "type" -> "function", "database" -> "samples")
      ))

      // must exist in __columns__
      val (_, _, results3) = QweryVM.searchSQL(scope2,
        """|select qname, `type`, columnName, columnType
           |from (OS.getDatabaseColumns())
           |where qname is 'samples.functions.calc_add'
           |""".stripMargin
      )
      results3.tabulate() foreach logger.info
      assert(results3.toMapGraph == List(
        Map("qname" -> "samples.functions.calc_add", "type" -> "function", "columnName" -> "a", "columnType" -> "Int"),
        Map("qname" -> "samples.functions.calc_add", "type" -> "function", "columnName" -> "b", "columnType" -> "Int")
      ))
    }

    it("should execute create a recursive function") {
      val (_, _, results4) = QweryVM.searchSQL(Scope(),
        """|namespace 'samples.functions'
           |drop if exists factorial
           |create function factorial(n: Int) := iff(@n <= 1, 1, @n * factorial(@n - 1))
           |select factorial(5)
           |""".stripMargin)
      results4.tabulate() foreach logger.info
      assert(results4.toMapGraph == List(Map("factorial" -> 120.0)))
    }

  }

}
