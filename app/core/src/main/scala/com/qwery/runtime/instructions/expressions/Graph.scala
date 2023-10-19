package com.qwery.runtime.instructions.expressions

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.{Expression, Queryable}
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Draws graphical charts
 * @param chart a dictionary containing the chart options
 * @param from  the data source to graph
 * @example {{{
 * graph { shape: "pie" } from @@exposure
 * }}}
 */
case class Graph(chart: Expression, from: Queryable) extends RuntimeExpression {

  override def evaluate()(implicit scope: Scope): GraphResult = {
    val (scope1, _, rc) = QweryVM.search(scope, from)
    val dict = chart.asDictionary(scope1) || dieXXXIsNull("Dictionary")
    dict.get("shape").collect { case s: String => s } || dieXXXIsNull("Attribute 'shape'")
    GraphResult(options = dict.toMap, data = rc)
  }

  override def toSQL: String = List("graph", chart.wrapSQL, from.toSQL).mkString(" ")

}

object Graph extends ExpressionParser {
  private val template = "graph %e:chart %i:source"

  override def help: List[HelpDoc] = List(
    HelpDoc(
      name = "graph",
      category = CATEGORY_DATAFRAME,
      paradigm = PARADIGM_DECLARATIVE,
      syntax = template,
      description = "Produces graphical charts",
      example =
        """|graph { shape: "ring", title: "Ring Demo" } from (
           |  |------------------|
           |  | exchange | total |
           |  |------------------|
           |  | NASDAQ   |    24 |
           |  | AMEX     |     5 |
           |  | NYSE     |    28 |
           |  | OTCBB    |    32 |
           |  | OTHEROTC |     7 |
           |  |------------------|
           |)
           |""".stripMargin
    ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Graph] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Option(Graph(chart = params.expressions("chart"), from = params.queryables("source")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "graph"

}