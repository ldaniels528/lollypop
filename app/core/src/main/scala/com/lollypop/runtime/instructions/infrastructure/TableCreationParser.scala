package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language._
import com.lollypop.language.models.{Literal, Queryable, TableModel}
import com.lollypop.runtime.DatabaseObjectRef

/**
 * Table Creation Parser Trait
 */
trait TableCreationParser extends ModifiableParser with IfNotExists {

  protected def parseTable(params: SQLTemplateParams, table: TableModel, ifNotExists: Boolean): TableCreation

  protected def parseTableFrom(params: SQLTemplateParams, table: TableModel, from: Queryable, ifNotExists: Boolean): TableCreation

  protected def parseTableLike(params: SQLTemplateParams, table: TableModel, template: DatabaseObjectRef, ifNotExists: Boolean): TableCreation

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[TableCreation] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      val ifNotExists = params.indicators.get("exists").contains(true)
      val template_? = params.locations.get("template")
      val table = TableModel(
        columns = params.parameters.getOrElse("columns", Nil).map(_.toColumn),
        initialCapacity = params.expressions.get("initialCapacity").map {
          case Literal(value: Number) => value.intValue()
          case x => dieUnsupportedType(x)
        },
        partitions = params.expressions.get("partitions"))

      // return the instruction
      val op_? = template_?.map(parseTableLike(params, table, _, ifNotExists))
      if (op_?.nonEmpty) op_? else {
        params.instructions.get("source") match {
          case None => Some(parseTable(params, table, ifNotExists))
          case Some(from: Queryable) => Some(parseTableFrom(params, table, from, ifNotExists))
          case Some(_) => ts.dieExpectedQueryable()
        }
      }
    } else None
  }

  def templateCard: String

}