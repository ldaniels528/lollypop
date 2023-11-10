package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.CATEGORY_CONCURRENCY
import com.lollypop.language.models.{Expression, Queryable}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.{Int32Type, StringType}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import lollypop.io.IOCost

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
  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (scope1, cost1, result1) = portExpr.execute(scope)
    val (scope2, cost2, result2) = sqlExpr.execute(scope1)
    val port = Int32Type.convert(result1)
    val sql = StringType.convert(result2)
    LollypopServers.evaluate(port, sql, scope2).get match {
      case Left(rc) => (scope, cost1 ++ cost2, rc)
      case Right(cost) => (scope, cost1 ++ cost2 ++ cost, cost)
    }
  }

}

object NodeExec extends FunctionCallParserE2(
  name = "nodeExec",
  category = CATEGORY_CONCURRENCY,
  description = "Executes a query on a running Lollypop peer node.",
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
