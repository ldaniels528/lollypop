package com.qwery.runtime.instructions.functions

import com.qwery.language.HelpDoc.CATEGORY_SYSTEM_TOOLS
import com.qwery.language.models.{Expression, Queryable}
import com.qwery.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.qwery.runtime.devices.RemoteRowCollection.getRemoteCollection
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.{DatabaseObjectRef, Scope}

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
case class NS(expression: Expression) extends ScalarFunctionCall with RuntimeExpression with Queryable {
  override val functionName: String = "ns"

  override def evaluate()(implicit scope: Scope): Any = {
    (expression.asString flatMap {
      case name if name.startsWith("//") => getRemoteCollection(name)
      case name => Option(scope.getUniverse.getReferencedEntity(DatabaseObjectRef(name).toNS))
    }).orNull
  }

}

object NS extends FunctionCallParserE1(
  name = "ns",
  category = CATEGORY_SYSTEM_TOOLS,
  description =
    """|Returns a persistent object (e.g. table, view, et al) from disk via a namespace
       |""".stripMargin,
  example = "from ns('examples.shocktrade.Contests') limit 5")
