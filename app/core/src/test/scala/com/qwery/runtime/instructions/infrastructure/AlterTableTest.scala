package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.Template
import com.qwery.language.TemplateProcessor.tags._
import com.qwery.language.models.Column
import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.infrastructure.AlterTable.{AddColumn, AppendColumn, DropColumn, PrependColumn, RenameColumn}
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import com.qwery.util.DateHelper
import com.qwery.util.StringHelper.StringEnrichment
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class AlterTableTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[AlterTable].getSimpleName) {

    it("should properly interpret optional sequences") {
      val template = Template("alter table %L:name %a:verb ?%C(type|column) %O {{ ?add +?%P:col ?append +?%P:col ?drop +?%a:col ?prepend +?%P:col ?rename +?%a:old +?as +?%a:new }}")
      template.tags foreach (tag => logger.info(s"tag: $tag"))
      assert(template.tags == List(
        KeywordTemplateTag("alter"),
        KeywordTemplateTag("table"),
        TableOrVariableTemplateTag("name"),
        AtomTemplateTag("verb"),
        OptionalTemplateTag(ChooseTemplateTag(Seq("type", "column")), Nil),
        OptionalSequenceTemplateTag(List(
          OptionalTemplateTag(KeywordTemplateTag("add"), List(OptionalDependentTemplateTag(ListOfParametersTemplateTag("col")))),
          OptionalTemplateTag(KeywordTemplateTag("append"), List(OptionalDependentTemplateTag(ListOfParametersTemplateTag("col")))),
          OptionalTemplateTag(KeywordTemplateTag("drop"), List(OptionalDependentTemplateTag(AtomTemplateTag("col")))),
          OptionalTemplateTag(KeywordTemplateTag("prepend"), List(OptionalDependentTemplateTag(ListOfParametersTemplateTag("col")))),
          OptionalTemplateTag(KeywordTemplateTag("rename"), List(OptionalDependentTemplateTag(AtomTemplateTag("old")), OptionalDependentTemplateTag(KeywordTemplateTag("as")), OptionalDependentTemplateTag(AtomTemplateTag("new"))))
        ))
      ))
    }

    it("should use its template to parse commands") {
      val template = Template(AlterTable.templateCard)
      template.tags foreach (t => logger.info(s"|${t.toCode.replace('\n', 222.toChar)}| ~> $t"))
      val params = template.processWithDebug(
        """|alter table stocks
           |  prepend column unit_id: UUID
           |  append column comments: String
           |""".stripMargin)
      params.all.foreach { case (k, v) => logger.info(s""""$k" -> "$v"""") }
      assert(params.all == Map(
        "name" -> DatabaseObjectRef("stocks"),
        "append_col" -> List("comments String".c),
        "prepend_col" -> List("unit_id UUID".c),
        "keywords" -> Set("column", "append", "prepend", "table", "alter")
      ))
    }

    it("should support compiling alter table .. add column .. default") {
      val model = compiler.compile("alter table stocks add column comments: String = 'N/A'")
      assert(model == AlterTable(DatabaseObjectRef("stocks"), AddColumn(Column("comments String").copy(defaultValue = Some("N/A".v)))))
    }

    it("should support compiling alter table .. append column") {
      val model = compiler.compile("alter table stocks append column comments: String")
      assert(model == AlterTable(DatabaseObjectRef("stocks"), AppendColumn(Column("comments String"))))
    }

    it("should support compiling alter table .. drop column") {
      val model = compiler.compile("alter table stocks drop column comments")
      assert(model == AlterTable(DatabaseObjectRef("stocks"), DropColumn("comments")))
    }

    it("should support compiling alter table .. prepend column") {
      val model = compiler.compile("alter table stocks prepend column comments: String")
      assert(model == AlterTable(DatabaseObjectRef("stocks"), PrependColumn(Column("comments String"))))
    }

    it("should support compiling alter table .. rename column") {
      val model = compiler.compile("alter table stocks rename column comments as remarks")
      assert(model == AlterTable(DatabaseObjectRef("stocks"), RenameColumn(oldName = "comments", newName = "remarks")))
    }

    it("should support compiling alter table .. add column/drop column") {
      val model = compiler.compile(
        """|alter table stocks
           |add column comments: String
           |drop column remarks
           |""".stripMargin)
      assert(model == AlterTable(DatabaseObjectRef("stocks"), Seq(AddColumn(Column("comments String")), DropColumn("remarks"))))
    }

    it("should support decompiling alter table .. add column .. default") {
      val model = AlterTable(DatabaseObjectRef("stocks"), Seq(AddColumn(Column("comments String").copy(defaultValue = Some("N/A".v)))))
      assert(model.toSQL ==
        """|alter table stocks
           |add column comments: String = "N/A"
           |""".stripMargin.singleLine)
    }

    it("should support decompiling alter table .. append column") {
      val model = AlterTable(DatabaseObjectRef("stocks"), Seq(AddColumn(Column("comments String"))))
      assert(model.toSQL ==
        """|alter table stocks
           |add column comments: String
           |""".stripMargin.singleLine)
    }

    it("should support decompiling alter table .. drop column") {
      val model = AlterTable(DatabaseObjectRef("stocks"), Seq(DropColumn("comments")))
      assert(model.toSQL ==
        """|alter table stocks
           |drop column comments
           |""".stripMargin.singleLine)
    }

    it("should support decompiling alter table .. prepend column") {
      val model = AlterTable(DatabaseObjectRef("stocks"), Seq(PrependColumn(Column("comments String"))))
      assert(model.toSQL ==
        """|alter table stocks
           |prepend column comments: String
           |""".stripMargin.singleLine)
    }

    it("should support decompiling alter table .. rename column") {
      val model = AlterTable(DatabaseObjectRef("stocks"), Seq(RenameColumn("comments", "remarks")))
      assert(model.toSQL ==
        """|alter table stocks
           |rename column comments as remarks
           |""".stripMargin.singleLine)
    }

    it("should support decompiling alter table .. add column/drop column") {
      val model = AlterTable(DatabaseObjectRef("stocks"), Seq(AddColumn(Column("comments String")), DropColumn("remarks")))
      assert(model.toSQL ==
        """|alter table stocks
           |add column comments: String
           |drop column remarks
           |""".stripMargin.singleLine)
    }

    it("should execute an alter table to add columns") {
      val (scope0, _, device0) = QweryVM.searchSQL(Scope(),
        """|namespace "temp.test"
           |drop if exists StockData
           |create table StockData (symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into StockData (symbol, exchange, lastSale) from (
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | XYZ    | AMEX     |    31.95 |
           |  | AAXX   | NYSE     |    56.12 |
           |  | QED    | NASDAQ   |          |
           |  | JUNK   | AMEX     |    97.61 |
           |  |------------------------------|
           |)
           |ns('StockData')
           |""".stripMargin)
      device0.tabulate().foreach(logger.info)
      assert(device0.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "QED", "exchange" -> "NASDAQ"),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61)
      ))

      val (_, _, device1) = QweryVM.searchSQL(scope0,
        """|alter table StockData append column saleDate: DateTime = DateTime('2023-06-20T03:52:14.543Z')
           |from ns('StockData')
           |""".stripMargin)
      device1.tabulate().foreach(logger.info)
      assert(device1.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("symbol" -> "QED", "exchange" -> "NASDAQ", "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z"))
      ))
    }

    it("should execute an alter table to add and rename columns") {
      val (scope0, _, device0) = QweryVM.searchSQL(Scope(),
        """|namespace "temp.test"
           |drop if exists StockData
           |create table StockData (symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into StockData (symbol, exchange, lastSale) from (
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | XYZ    | AMEX     |    31.95 |
           |  | AAXX   | NYSE     |    56.12 |
           |  | QED    | NASDAQ   |          |
           |  | JUNK   | AMEX     |    97.61 |
           |  |------------------------------|
           |)
           |ns('StockData')
           |""".stripMargin)
      device0.tabulate().foreach(logger.info)
      assert(device0.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "QED", "exchange" -> "NASDAQ"),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61)
      ))

      val (_, _, device1) = QweryVM.searchSQL(scope0,
        """|alter table StockData
           |  rename column exchange as market
           |  add column saleDate: DateTime = DateTime('2023-06-20T03:52:14.543Z')
           |from ns('StockData')
           |""".stripMargin)
      device1.tabulate().foreach(logger.info)
      assert(device1.toMapGraph == List(
        Map("symbol" -> "XYZ", "market" -> "AMEX", "lastSale" -> 31.95, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("symbol" -> "AAXX", "market" -> "NYSE", "lastSale" -> 56.12, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("symbol" -> "QED", "market" -> "NASDAQ", "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("symbol" -> "JUNK", "market" -> "AMEX", "lastSale" -> 97.61, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z"))
      ))
    }

    it("should execute an alter table to prepend, append and rename columns") {
      val (scope0, _, device0) = QweryVM.searchSQL(Scope(),
        """|namespace "temp.test"
           |drop if exists StockData
           |create table StockData (symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into StockData (symbol, exchange, lastSale) from (
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | XYZ    | AMEX     |    31.95 |
           |  | AAXX   | NYSE     |    56.12 |
           |  | QED    | NASDAQ   |          |
           |  | JUNK   | AMEX     |    97.61 |
           |  |------------------------------|
           |)
           |ns('StockData')
           |""".stripMargin)
      device0.tabulate().foreach(logger.info)
      assert(device0.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "QED", "exchange" -> "NASDAQ"),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61)
      ))

      val (_, _, device1) = QweryVM.searchSQL(scope0,
        """|alter table StockData
           |  prepend column saleDate: DateTime = DateTime('2023-06-20T03:52:14.543Z')
           |  append column beta: Double = 0.5
           |  rename column symbol as ticker
           |from ns('StockData')
           |""".stripMargin)
      device1.tabulate().foreach(logger.info)
      assert(device1.toMapGraph == List(
        Map("ticker" -> "XYZ", "exchange" -> "AMEX", "beta" -> 0.5, "lastSale" -> 31.95, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("ticker" -> "AAXX", "exchange" -> "NYSE", "beta" -> 0.5, "lastSale" -> 56.12, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("ticker" -> "QED", "exchange" -> "NASDAQ", "beta" -> 0.5, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("ticker" -> "JUNK", "exchange" -> "AMEX", "beta" -> 0.5, "lastSale" -> 97.61, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z"))
      ))
    }

    it("should execute an alter table to prepend, append, drop and rename columns") {
      val (scope0, _, device0) = QweryVM.searchSQL(Scope(),
        """|namespace "temp.test"
           |drop if exists StockData
           |create table StockData (symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into StockData (symbol, exchange, lastSale) from (
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | XYZ    | AMEX     |    31.95 |
           |  | AAXX   | NYSE     |    56.12 |
           |  | QED    | NASDAQ   |          |
           |  | JUNK   | AMEX     |    97.61 |
           |  |------------------------------|
           |)
           |ns('StockData')
           |""".stripMargin)
      device0.tabulate().foreach(logger.info)
      assert(device0.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "QED", "exchange" -> "NASDAQ"),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61)
      ))

      val (_, _, device1) = QweryVM.searchSQL(scope0,
        """|alter table StockData
           |  prepend column saleDate: DateTime = DateTime('2023-06-20T03:52:14.543Z')
           |  append column beta: Double = 1.0
           |  drop column lastSale
           |  rename column symbol as ticker
           |from ns('StockData')
           |""".stripMargin)
      device1.tabulate().foreach(logger.info)
      assert(device1.toMapGraph == List(
        Map("ticker" -> "XYZ", "exchange" -> "AMEX", "beta" -> 1.0, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("ticker" -> "AAXX", "exchange" -> "NYSE", "beta" -> 1.0, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("ticker" -> "QED", "exchange" -> "NASDAQ", "beta" -> 1.0, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("ticker" -> "JUNK", "exchange" -> "AMEX", "beta" -> 1.0, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z"))
      ))
    }

    it("should execute an alter table to prepend, add and rename columns in memory") {
      val (scope0, _, device0) = QweryVM.searchSQL(Scope(),
        """|declare table stocks(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @@stocks (symbol, exchange, lastSale) from (
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | XYZ    | AMEX     |    31.95 |
           |  | AAXX   | NYSE     |    56.12 |
           |  | QED    | NASDAQ   |          |
           |  | JUNK   | AMEX     |    97.61 |
           |  |------------------------------|
           |)
           |stocks
           |""".stripMargin)
      device0.tabulate().foreach(logger.info)
      assert(device0.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "QED", "exchange" -> "NASDAQ"),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61)
      ))

      val (_, _, device1) = QweryVM.searchSQL(scope0,
        """|alter table @@stocks
           |  prepend column saleDate: DateTime = DateTime('2023-06-20T03:52:14.543Z')
           |  rename column symbol as ticker
           |stocks
           |""".stripMargin)
      device1.tabulate().foreach(logger.info)
      assert(device1.toMapGraph == List(
        Map("ticker" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("ticker" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("ticker" -> "QED", "exchange" -> "NASDAQ", "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z")),
        Map("ticker" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61, "saleDate" -> DateHelper("2023-06-20T03:52:14.543Z"))
      ))
    }

  }

}
