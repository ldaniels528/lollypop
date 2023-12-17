package com.lollypop.runtime.instructions.functions

import com.lollypop.language.HelpDoc.CATEGORY_SYSTEM_TOOLS
import com.lollypop.language.models.{Expression, Queryable}
import com.lollypop.runtime._
import com.lollypop.runtime.devices.RemoteRowCollection.getRemoteCollection
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
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
  override val name: String = "ns"

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (sa, ca, text) = expression.pullString
    (sa, ca, text match {
      case name if name.startsWith("//") => getRemoteCollection(name)
      case name => Option(scope.getUniverse.getReferencedEntity(DatabaseObjectRef(name).toNS))
    })
  }

}

object NS extends FunctionCallParserE1(
  name = "ns",
  category = CATEGORY_SYSTEM_TOOLS,
  description =
    """|Returns a persistent object (e.g. table, view, et al) from disk via a namespace
       |""".stripMargin,
  example = "from ns('lollypop.public.Stocks') limit 5")
