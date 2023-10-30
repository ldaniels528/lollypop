package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.IfNotExists.IfNotExistsTemplateTag
import com.lollypop.language.TemplateProcessor.tags._
import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.language.models.View
import com.lollypop.language.{SQLTemplateParams, Template}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import com.lollypop.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class CreateViewTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[CreateView].getSimpleName) {

    it("support properly interpret create view template") {
      val template = Template("create view ?%IFNE:exists %L:ref ?as %Q:query")
      template.tags foreach (tag => logger.info(s"tag: $tag"))
      assert(template.tags == List(
        KeywordTemplateTag("create"),
        KeywordTemplateTag("view"),
        OptionalTemplateTag(IfNotExistsTemplateTag("exists")),
        TableOrVariableTemplateTag("ref"),
        OptionalTemplateTag(KeywordTemplateTag("as")),
        QueryOrVariableTemplateTag("query")
      ))
    }

    it("should use the template to parse statements") {
      val template = Template(CreateView.template)
      val params = template.process(
        """|create view if not exists OilAndGas
           |as
           |  select Symbol, Name, Sector, Industry, SummaryQuote
           |  from Customers
           |  where Industry is 'Oil/Gas Transmission'
           |""".stripMargin)
      params.all foreach { case (k, v) => logger.info(s"param: $k => $v") }
      assert(params == new SQLTemplateParams(
        indicators = Map("exists" -> true),
        atoms = Map("_" -> "as"),
        keywords = Set("create", "view"),
        locations = Map("ref" -> DatabaseObjectRef("OilAndGas")),
        instructions = Map("query" -> Select(
          fields = Seq("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
          from = DatabaseObjectRef("Customers"),
          where = "Industry".f is "Oil/Gas Transmission"
        ))
      ))
    }

    it("should support create view .. comment is") {
      val results = compiler.compile(
        """|create view if not exists OilAndGas
           |as
           |select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where Industry == 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == CreateView(ref = DatabaseObjectRef("OilAndGas"), View(
        query = Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
          from = DatabaseObjectRef("Customers"),
          where = "Industry".f === "Oil/Gas Transmission"
        )), ifNotExists = true))
    }

    it("should support create view") {
      val results = compiler.compile(
        """|create view if not exists OilAndGas as
           |  select Symbol, Name, Sector, Industry, SummaryQuote
           |  from Customers
           |  where Industry is 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == CreateView(ref = DatabaseObjectRef("OilAndGas"), View(
        query = Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
          from = DatabaseObjectRef("Customers"),
          where = "Industry".f is "Oil/Gas Transmission"
        )), ifNotExists = true))
    }

    it("should support decompiling create view") {
      val model = CreateView(ref = DatabaseObjectRef("OilAndGas"), View(
        query = Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
          from = DatabaseObjectRef("Customers"),
          where = "Industry".f === "Oil/Gas Transmission"
        )), ifNotExists = true)
      assert(model.toSQL ==
        """create view if not exists OilAndGas := select Symbol, Name, Sector, Industry, SummaryQuote from Customers where Industry == "Oil/Gas Transmission"""")
    }

    it(s"should query rows from view stocks_view") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(), sql =
        s"""|namespace 'test.views'
            |drop if exists stocks_view
            |create view stocks_view
            |as
            |from (
            |    |---------------------------------------------------------|
            |    | symbol | exchange | lastSale | lastSaleTime             |
            |    |---------------------------------------------------------|
            |    | ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
            |    | ABC    | OTCBB    |   8.1112 | 2022-09-04T09:36:51.007Z |
            |    | BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
            |    | TREE   | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
            |    | AQKU   | NASDAQ   |  68.2945 | 2022-09-04T09:36:51.112Z |
            |    | BKBK   | NASDAQ   |  78.1238 | 2022-09-04T09:36:47.080Z |
            |    | NGA    | NASDAQ   |  23.6812 | 2022-09-04T09:36:41.808Z |
            |    | TRX    | NYSE     |  88.22   | 2022-09-04T09:12:53.009Z |
            |    | TRX    | NYSE     |  88.56   | 2022-09-04T09:12:57.706Z |
            |    | NGINX  | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
            |    | WRKR   | AMEX     |  46.8355 | 2022-09-04T09:36:48.111Z |
            |    | ESCN   | AMEX     |  42.5934 | 2022-09-04T09:36:42.321Z |
            |    | NFRK   | NYSE     |  28.2808 | 2022-09-04T09:36:47.675Z |
            |    | AAPL   | NASDAQ   | 100.01   | 2022-09-04T09:36:46.033Z |
            |    | AAPL   | NASDAQ   | 100.12   | 2022-09-04T09:36:48.459Z |
            |    | WRKR   | AMEX     | 100.12   | 2022-09-04T09:36:48.459Z |
            |    | BOOTY  | OTCBB    |  13.12   | 2022-09-04T09:51:13.111Z |
            |    |---------------------------------------------------------|
            |)
            |select * from stocks_view limit 5
            |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.0985, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.846Z")),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.1112, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.007Z")),
        Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 17.5776, "lastSaleTime" -> DateHelper("2022-09-04T09:37:11.332Z")),
        Map("symbol" -> "TREE", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "AQKU", "exchange" -> "NASDAQ", "lastSale" -> 68.2945, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.112Z"))
      ))
    }

  }

}
