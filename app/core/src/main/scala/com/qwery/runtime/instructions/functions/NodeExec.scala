package com.qwery.runtime.instructions.functions

import com.qwery.database.server.QweryServers
import com.qwery.language.HelpDoc.CATEGORY_DISTRIBUTED
import com.qwery.language.models.{Expression, Queryable}
import com.qwery.runtime.datatypes.{Int32Type, StringType}
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.{QweryVM, Scope}

/**
 * NodeExec
 * @example {{{
 *   set port = nodeStart()
 *   nodeExec(port, "files")
 *   nodeStop(port)
 * }}}
 * @param portExpr the port [[Expression]]
 * @param sqlExpr  the SQL [[Expression]]
 */
case class NodeExec(portExpr: Expression, sqlExpr: Expression) extends ScalarFunctionCall
  with RuntimeExpression with Queryable {
  override def evaluate()(implicit scope: Scope): Any = {
    val (scope1, cost1, result1) = QweryVM.execute(scope, portExpr)
    val (scope2, cost2, result2) = QweryVM.execute(scope1, sqlExpr)
    val port = Int32Type.convert(result1)
    val sql = StringType.convert(result2)
    QweryServers.evaluate(port, sql, scope2).get match {
      case Left(rc) => rc
      case Right(cost) => cost
    }
  }

}

object NodeExec extends FunctionCallParserE2(
  name = "nodeExec",
  category = CATEGORY_DISTRIBUTED,
  description = "Executes a query on a running Qwery peer node.",
  example =
    """|val port = nodeStart()
       |after Interval('4 seconds') nodeStop(port)
       |nodeExec(port, '''
       |from (
       |    |-------------------------------------------------------|
       |    | ticker | market | lastSale | lastSaleTime             |
       |    |-------------------------------------------------------|
       |    | NKWI   | OTCBB  |  98.9501 | 2022-09-04T23:36:47.846Z |
       |    | AQKU   | NASDAQ |  68.2945 | 2022-09-04T23:36:47.860Z |
       |    | WRGB   | AMEX   |  46.8355 | 2022-09-04T23:36:47.862Z |
       |    | ESCN   | AMEX   |  42.5934 | 2022-09-04T23:36:47.865Z |
       |    | NFRK   | AMEX   |  28.2808 | 2022-09-04T23:36:47.864Z |
       |    |-------------------------------------------------------|
       |) where lastSale < 30
       |''')
       |""".stripMargin)
