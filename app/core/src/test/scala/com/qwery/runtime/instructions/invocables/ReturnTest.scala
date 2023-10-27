package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Operation.RichOperation
import com.qwery.language.models.{@@, @@@, AllFields}
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import com.qwery.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec

class ReturnTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Return].getSimpleName) {

    it("should compile return") {
      assert(compiler.compile("return") == Return())
    }

    it("should compile return salesAmount * 0.5") {
      assert(compiler.compile("return salesAmount * 0.5") == Return("salesAmount".f * 0.5))
    }

    it("should compile return @x*2") {
      assert(compiler.compile("return @x*2") == Return(@@("x") * 2))
    }

    it("should compile return @@stocks") {
      assert(compiler.compile("return @@stocks") == Return(@@@("stocks")))
    }

    it("should compile return (select * from stocks)") {
      assert(compiler.compile("return (select * from stocks)") == Return(Select(
        fields = Seq(AllFields),
        from = DatabaseObjectRef("stocks"))))
    }

    it("should execute return") {
      val (_, _, result_?) = QweryVM.executeSQL(Scope(), sql = "return")
      assert(result_? == null)
    }

    it("should execute return @x*2") {
      val (_, _, returned) = QweryVM.executeSQL(Scope(), sql =
        """|set x = {
           |  set @a = 3
           |  return @a * 2
           |}
           |stdout <=== '@x = {{x}}'
           |return @x
           |""".stripMargin)
      assert(returned contains 6.0)
    }

    it("should execute return (select ... )") {
      val (_, _, result_?) = QweryVM.executeSQL(Scope(),
        """|return (select name from (from OS.listFiles('./contrib/examples/src/main/qwery')) where name like '%.sql' order by name)
           |""".stripMargin)
      val device_? = result_?.collect { case d: RowCollection => d }
      assert(device_?.toList.flatMap(_.toMapGraph).toSet == Set(
        Map("name" -> "BlackJack.sql"),
        Map("name" -> "BreakOutDemo.sql"),
        Map("name" -> "GenerateVinMapping.sql"),
        Map("name" -> "IngestDemo.sql"),
        Map("name" -> "MacroDemo.sql"),
        Map("name" -> "Stocks.sql"),
        Map("name" -> "SwingDemo.sql")
      ))
    }

  }

}