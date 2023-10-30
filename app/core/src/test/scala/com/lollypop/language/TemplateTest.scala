package com.lollypop.language

import com.lollypop.language.InsertValues.InsertSourceTemplateTag
import com.lollypop.language.TemplateProcessor.tags._
import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.runtime.instructions.queryables.RowsOfValues
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler}
import org.scalatest.funspec.AnyFunSpec

class TemplateTest extends AnyFunSpec {
  private implicit val compiler: LollypopCompiler = LollypopCompiler()

  TemplateProcessor.addTag("V", InsertSourceTemplateTag)

  describe(classOf[TemplateProcessor].getSimpleName) {

    it("should parse: 'upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition'") {
      val template = Template("upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition")
      assert(template == Template(
        KeywordTemplateTag("upsert"), KeywordTemplateTag("into"), TableOrVariableTemplateTag("target"),
        OptionalTemplateTag(KeywordTemplateTag("("), List(
          OptionalDependentTemplateTag(ListOfFieldsTemplateTag("fields")),
          OptionalDependentTemplateTag(KeywordTemplateTag(")"))
        )),
        InsertSourceTemplateTag("source"),
        KeywordTemplateTag("where"),
        ConditionTemplateTag("condition")
      ))
    }

    it("should indicate when a string matches a template") {
      val template = Template("upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition")
      val sql =
        """|upsert into Stocks (symbol, exchange, lastSale)
           |values ('AAPL', 'NASDAQ', 156.39)
           |where symbol is 'AAPL'
           |""".stripMargin
      assert(template.matches(sql))
    }

    it("should indicate when a string does not match a template") {
      val template = Template("upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition")
      val sql =
        """|upsert into Stocks (symbol, exchange, lastSale)
           |values ('AAPL', 'NASDAQ', 156.39)
           |when symbol is 'AAPL'
           |""".stripMargin
      assert(!template.matches(sql))
    }

    it("should process a template") {
      val template = Template("upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition")
      val params = template.process(
        """|upsert into Stocks (symbol, exchange, lastSale)
           |values ('AAPL', 'NASDAQ', 156.39)
           |where symbol is 'AAPL'
           |""".stripMargin)
      assert(params.all == Map(
        "source" -> RowsOfValues(List(List("AAPL".v, "NASDAQ".v, 156.39.v))),
        "condition" -> ("symbol".f is "AAPL".v),
        "fields" -> List("symbol".f, "exchange".f, "lastSale".f),
        "keywords" -> Set("where", "upsert", ")", "into", "("), "target" -> DatabaseObjectRef("Stocks")
      ))
    }

    it("should process a template (with debugging)") {
      val template = Template("upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition")
      val params = template.processWithDebug(
        """|upsert into Stocks (symbol, exchange, lastSale)
           |values ('AAPL', 'NASDAQ', 156.39)
           |where symbol is 'AAPL'
           |""".stripMargin)
      assert(params.all == Map(
        "source" -> RowsOfValues(List(List("AAPL".v, "NASDAQ".v, 156.39.v))),
        "condition" -> ("symbol".f is "AAPL".v),
        "fields" -> List("symbol".f, "exchange".f, "lastSale".f),
        "keywords" -> Set("where", "upsert", ")", "into", "("), "target" -> DatabaseObjectRef("Stocks")
      ))
    }

    it("should produce an appropriate toString() value") {
      val template = Template("upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition")
      assert(template.toString == """Template("upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition")""")
    }

  }

}
