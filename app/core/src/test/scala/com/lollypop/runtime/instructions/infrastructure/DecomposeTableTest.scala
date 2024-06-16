package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.LifestyleExpressions
import com.lollypop.language.implicits._
import com.lollypop.runtime.datatypes.{Int16Type, StringType}
import com.lollypop.runtime.devices.TableColumn
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.runtime.instructions.functions.Histogram
import com.lollypop.runtime.instructions.queryables.TableVariableRef
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVMAddOns, Scope}
import org.scalatest.funspec.AnyFunSpec

/**
 * Decompose Table tests
 */
class DecomposeTableTest extends AnyFunSpec with VerificationTools {
  private implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[DecomposeTable].getSimpleName) {

    it("should compile a decompose table statement (variable, no transforms)") {
      val model = compiler.compile(
        """|decompose table @stocks []
           |""".stripMargin)
      assert(model == DecomposeTable(TableVariableRef("stocks"), ArrayLiteral()))
    }

    it("should compile a decompose table statement (variable)") {
      val model = compiler.compile(
        """|decompose table @stocks [
           |   Histogram(last_sale, [0, 1, 5, 10, 20, 100, 250, 1000])
           |]
           |""".stripMargin)
      assert(model == DecomposeTable(
        TableVariableRef("stocks"),
        ArrayLiteral(Histogram("last_sale".f, ArrayLiteral(0, 1, 5, 10, 20, 100, 250, 1000)))))
    }

    it("should compile a decompose table statement (durable)") {
      val model = compiler.compile(
        """|decompose table stocks [
           |   Histogram(last_sale, [0, 1, 5, 10, 20, 100, 250, 1000])
           |]
           |""".stripMargin)
      assert(model == DecomposeTable(
        DatabaseObjectRef("stocks"),
        ArrayLiteral(Histogram("last_sale".f, ArrayLiteral(0, 1, 5, 10, 20, 100, 250, 1000)))))
    }

    it("should create a table model decomposed copy of a table structure") {
      val model = compiler.compile(
        """|declare table stocks(
           |  symbol: String(8),
           |  exchange: Enum(AMEX, NASDAQ, NYSE, OTC_BB, OTHER_OTC),
           |  last_sale: Double
           |)
           |insert into @stocks
           |  |--------------------------------|
           |  | symbol | exchange  | last_sale |
           |  |--------------------------------|
           |  | ABC    | OTC_BB    |      0.12 |
           |  | JET    | NASDAQ    |     41.26 |
           |  | APAC   | NYSE      |    116.24 |
           |  | XYZ    | AMEX      |     31.95 |
           |  | JUNK   | OTHER_OTC |     0.411 |
           |  |--------------------------------|
           |decompose table @stocks [
           |   Histogram(last_sale, [0, 1, 5, 10, 20, 100, 250, 1000])
           |]
           |""".stripMargin)

      val (_, _, rc) = model.search(Scope())
      assert(rc.columns == List(
        TableColumn(name = "symbol", `type` = StringType(8)),
        TableColumn(name = "is_amex", `type` = Int16Type),
        TableColumn(name = "is_nasdaq", `type` = Int16Type),
        TableColumn(name = "is_nyse", `type` = Int16Type),
        TableColumn(name = "is_otc_bb", `type` = Int16Type),
        TableColumn(name = "is_other_otc", `type` = Int16Type),
        TableColumn(name = "last_sale", `type` = Int16Type),
      ))

      rc.tabulate().foreach(println)
      assert(rc.toMapGraph == List(
        Map("symbol" -> "ABC", "is_amex" -> 0, "is_nasdaq" -> 0, "is_nyse" -> 0, "is_otc_bb" -> 1, "is_other_otc" -> 0, "last_sale" -> 0),
        Map("symbol" -> "JET", "is_amex" -> 0, "is_nasdaq" -> 1, "is_nyse" -> 0, "is_otc_bb" -> 0, "is_other_otc" -> 0, "last_sale" -> 4),
        Map("symbol" -> "APAC", "is_amex" -> 0, "is_nasdaq" -> 0, "is_nyse" -> 1, "is_otc_bb" -> 0, "is_other_otc" -> 0, "last_sale" -> 5),
        Map("symbol" -> "XYZ", "is_amex" -> 1, "is_nasdaq" -> 0, "is_nyse" -> 0, "is_otc_bb" -> 0, "is_other_otc" -> 0, "last_sale" -> 4),
        Map("symbol" -> "JUNK", "is_amex" -> 0, "is_nasdaq" -> 0, "is_nyse" -> 0, "is_otc_bb" -> 0, "is_other_otc" -> 1, "last_sale" -> 0)
      ))
    }

    it("should produce null for an undefined value") {
      val model = compiler.compile(
        """|declare table stocks(
           |  symbol: String(8),
           |  exchange: Enum(AMEX, NASDAQ, NYSE, OTC_BB, OTHER_OTC),
           |  last_sale: Double
           |)
           |insert into @stocks
           |  |--------------------------------|
           |  | symbol | exchange  | last_sale |
           |  |--------------------------------|
           |  | ABC    | OTC_BB    |      0.12 |
           |  | JET    | NASDAQ    |     41.26 |
           |  | APAC   | NYSE      |    116.24 |
           |  | XYZ    | AMEX      |     31.95 |
           |  | JUNK   | OTHER_OTC |     0.411 |
           |  |--------------------------------|
           |decompose table @stocks [
           |   Histogram(last_sale, [0, 1, 5, 10, 20, 100])
           |]
           |""".stripMargin)

      val (_, _, rc) = model.search(Scope())
      rc.tabulate().foreach(println)
      assert(rc.toMapGraph == List(
        Map("symbol" -> "ABC", "is_amex" -> 0, "is_nasdaq" -> 0, "is_nyse" -> 0, "is_otc_bb" -> 1, "is_other_otc" -> 0, "last_sale" -> 0),
        Map("symbol" -> "JET", "is_amex" -> 0, "is_nasdaq" -> 1, "is_nyse" -> 0, "is_otc_bb" -> 0, "is_other_otc" -> 0, "last_sale" -> 4),
        Map("symbol" -> "APAC", "is_amex" -> 0, "is_nasdaq" -> 0, "is_nyse" -> 1, "is_otc_bb" -> 0, "is_other_otc" -> 0),
        Map("symbol" -> "XYZ", "is_amex" -> 1, "is_nasdaq" -> 0, "is_nyse" -> 0, "is_otc_bb" -> 0, "is_other_otc" -> 0, "last_sale" -> 4),
        Map("symbol" -> "JUNK", "is_amex" -> 0, "is_nasdaq" -> 0, "is_nyse" -> 0, "is_otc_bb" -> 0, "is_other_otc" -> 1, "last_sale" -> 0)
      ))
    }

  }

}
