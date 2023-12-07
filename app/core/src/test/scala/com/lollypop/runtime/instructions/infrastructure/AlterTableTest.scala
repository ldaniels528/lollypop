package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.TemplateProcessor.tags._
import com.lollypop.language.models.Column
import com.lollypop.language.{Template, _}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.infrastructure.AlterTable._
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import lollypop.io.{IOCost, RowIDRange}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class AlterTableTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val compiler: LollypopCompiler = LollypopCompiler()

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
      val model = compiler.compile("alter table stocks rename column comments to remarks")
      assert(model == AlterTable(DatabaseObjectRef("stocks"), RenameColumn(oldName = "comments", newName = "remarks")))
    }

    it("should support compiling alter table .. set description") {
      val model = compiler.compile("alter table stocks label 'This is a test'")
      assert(model == AlterTable(DatabaseObjectRef("stocks"), SetLabel(description = "This is a test")))
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
           |rename column comments to remarks
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
      val (scope0, _, device0) = LollypopVM.searchSQL(Scope(),
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

      val (_, _, device1) = LollypopVM.searchSQL(scope0,
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
      val (scope0, _, device0) = LollypopVM.searchSQL(Scope(),
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

      val (_, _, device1) = LollypopVM.searchSQL(scope0,
        """|alter table StockData
           |  rename column exchange to market
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
      val (scope0, _, device0) = LollypopVM.searchSQL(Scope(),
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

      val (_, _, device1) = LollypopVM.searchSQL(scope0,
        """|alter table StockData
           |  prepend column saleDate: DateTime = DateTime('2023-06-20T03:52:14.543Z')
           |  append column beta: Double = 0.5
           |  rename column symbol to ticker
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
      val (scope0, _, device0) = LollypopVM.searchSQL(Scope(),
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

      val (_, _, device1) = LollypopVM.searchSQL(scope0,
        """|alter table StockData
           |  prepend column saleDate: DateTime = DateTime('2023-06-20T03:52:14.543Z')
           |  append column beta: Double = 1.0
           |  drop column lastSale
           |  rename column symbol to ticker
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
      val (scope0, _, device0) = LollypopVM.searchSQL(Scope(),
        """|declare table stocks(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @stocks (symbol, exchange, lastSale) from (
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

      val (_, _, device1) = LollypopVM.searchSQL(scope0,
        """|alter table @stocks
           |  prepend column saleDate: DateTime = DateTime('2023-06-20T03:52:14.543Z')
           |  rename column symbol to ticker
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

    it("should execute an alter table to prepend, rename columns and label a durable collection") {
      val xStockQuotes = DatabaseObjectRef("StockQuotes")
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|namespace "temp.examples"
            |drop if exists $xStockQuotes
            |create table $xStockQuotes(symbol: String(5), exchange: String(9), lastSale: Double) containing (
            ||----------------------------------------------------------|
            || exchange  | symbol | lastSale | lastSaleTime             |
            ||----------------------------------------------------------|
            || OTCBB     | YSZUY  |   0.2355 | 2023-10-19T23:25:32.886Z |
            || NASDAQ    | DMZH   | 183.1636 | 2023-10-19T23:26:03.509Z |
            || OTCBB     | VV     |          |                          |
            || NYSE      | TGPNF  |  51.6171 | 2023-10-19T23:25:32.166Z |
            || OTHER_OTC | RIZA   |   0.2766 | 2023-10-19T23:25:42.020Z |
            || NASDAQ    | JXMLB  |  91.6028 | 2023-10-19T23:26:08.951Z |
            ||----------------------------------------------------------|
            |)
            |alter table $xStockQuotes
            |  prepend column saleDate: DateTime = DateTime()
            |  rename column symbol to ticker
            |  label 'Stock quotes staging table'
            |""".stripMargin)
      val ns = xStockQuotes.toNS(scope)
      assert(ns.getConfig.description contains "Stock quotes staging table")
      assert(cost == IOCost(created = 1, destroyed = 1, inserted = 18, rowIDs = RowIDRange(0L to 5L: _*)))
    }

  }

}
