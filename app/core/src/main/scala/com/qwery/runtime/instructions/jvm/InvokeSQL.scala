package com.qwery.runtime.instructions.jvm

import com.qwery.database.QueryResponse
import com.qwery.database.QueryResponse.QueryResultConversion
import com.qwery.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_IMPERATIVE}
import com.qwery.language.models.Expression
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.qwery.runtime.instructions.queryables.RuntimeQueryable.DatabaseObjectRefDetection
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Execute a SQL statement returning the results as a [[QueryResponse]] object.
 * @param expression the [[Expression expression]] representing the SQL code
 */
case class InvokeSQL(expression: Expression) extends ScalarFunctionCall with RuntimeExpression {
  override def evaluate()(implicit scope: Scope): QueryResponse = {
    val sql = expression.asString || expression.die("String value expected")
    val compiledCode = scope.getCompiler.compile(sql)
    val ns_? = compiledCode.detectRef.collect { case ref: DatabaseObjectRef => ref.toNS }
    val (_, _, result1) = QweryVM.execute(scope, compiledCode)
    result1.toQueryResponse(ns_?, limit = None)
  }
}

object InvokeSQL extends FunctionCallParserE1(
  name = "invokeSQL",
  category = CATEGORY_CONTROL_FLOW,
  paradigm = PARADIGM_IMPERATIVE,
  description = s"Execute a SQL statement returning the results as a `${classOf[QueryResponse].getName}` object.",
  example =
    """|stocks =
       ||---------------------------------------------------------|
       || symbol | exchange | lastSale | lastSaleTime             |
       ||---------------------------------------------------------|
       || FEUA   | OTCBB    |    0.473 | 2023-07-29T04:59:50.709Z |
       || QMBF   | OTCBB    |   2.4309 | 2023-07-29T04:59:50.727Z |
       || CVWO   | AMEX     |  29.4932 | 2023-07-29T04:59:50.728Z |
       || CMJF   | AMEX     | 161.3559 | 2023-07-29T04:59:50.730Z |
       || BETH   | NASDAQ   |  37.7045 | 2023-07-29T04:59:50.731Z |
       ||---------------------------------------------------------|
       |invokeSQL("from stocks where symbol is 'BETH'")
       |""".stripMargin)


