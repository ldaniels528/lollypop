package com.lollypop.runtime.instructions.functions

import com.lollypop.language.HelpDoc.CATEGORY_SYSTEM_TOOLS
import com.lollypop.language.models.{Expression, Queryable}
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.devices.RemoteRowCollection.getRemoteCollection
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

/**
 * The NS() function
 * @example {{{
 *  val stocks = ns("ldaniels.portfolio.stocks")
 * }}}
 * @example {{{
 *  val remoteStocks = ns("//0.0.0.0:8888/ldaniels.portfolio.stocks")
 * }}}
 * @param expression the [[Expression expression]]
 */
case class NS(expression: Expression) extends ScalarFunctionCall
  with RuntimeExpression with Queryable {
  override val functionName: String = "ns"

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val result = (expression.asString flatMap {
      case name if name.startsWith("//") => getRemoteCollection(name)
      case name => Option(scope.getUniverse.getReferencedEntity(DatabaseObjectRef(name).toNS))
    }).orNull
    (scope, IOCost.empty, result)
  }

}

object NS extends FunctionCallParserE1(
  name = "ns",
  category = CATEGORY_SYSTEM_TOOLS,
  description =
    """|Returns a persistent object (e.g. table, view, et al) from disk via a namespace
       |""".stripMargin,
  example = "from ns('lollypop.public.Stocks') limit 5")
