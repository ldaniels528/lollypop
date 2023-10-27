package com.qwery.runtime.instructions.queryables

import com.qwery.implicits.MagicBoolImplicits
import com.qwery.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_DECLARATIVE}
import com.qwery.language._
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.{StringType, TableType}
import com.qwery.runtime.devices.{RowCollection, TableColumn}
import com.qwery.runtime.instructions.expressions.{RuntimeExpression, TableExpression}

/**
 * This (scope variable)
 */
case class This() extends RuntimeExpression with TableRendering with TableExpression {

  override def evaluate()(implicit scope: Scope): Any = scope

  override def toSQL: String = "this"

  override def returnType: TableType = toTableType

  override def toTable(implicit scope: Scope): RowCollection = scope.toRowCollection

  override def toTableType: TableType = TableType(Seq(
    TableColumn(name = "name", `type` = StringType),
    TableColumn(name = "value", `type` = StringType),
    TableColumn(name = "kind", `type` = StringType)
  ))

}

object This extends ExpressionParser {
  private val templateCard: String = "this"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "this",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Table representation of the current scope",
    example = "this"
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[This] = {
    ts.nextIf(templateCard) ==> This()
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is templateCard

}
