package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_IO, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.Instruction
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.datatypes.{BooleanType, StringType, TableType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.runtime.instructions.expressions.TableExpression
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

/**
 * SQL describe `query`
 * @param queryable the [[Instruction query]] to describe
 */
case class Describe(queryable: Instruction) extends RuntimeQueryable with TableExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scope1, cost1, result1) = queryable.search(scope)
    val columns = result1.columns
    implicit val out: RowCollection = createQueryResultTable(columns = returnType.columns)
    val cost2 = columns map { column =>
      val row = Map("name" -> column.name, "type" -> column.`type`.toSQL, "defaultValue" -> column.defaultValue)
      out.insert(row.toRow)
    } reduce (_ ++ _)
    (scope, cost1 ++ cost2, out)
  }

  override def toSQL: String = queryable match {
    case ref: DatabaseObjectRef => s"describe ${ref.toSQL}"
    case queryable => s"describe (${queryable.toSQL})"
  }

  override def returnType: TableType = TableType(columns = Seq(
    TableColumn(name = "name", `type` = StringType),
    TableColumn(name = "type", `type` = StringType),
    TableColumn(name = "description", `type` = StringType),
    TableColumn(name = "defaultValue", `type` = StringType),
    TableColumn(name = "isNullable", `type` = BooleanType)
  ))

}

object Describe extends QueryableParser {

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "describe",
    category = CATEGORY_DATAFRAMES_IO,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a table representing the layout of the query expression",
    example = "describe (select v1: 123, v2: 'abc')"
  ))

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Describe] = {
    if (understands(ts)) {
      Some(Describe(SQLTemplateParams(ts, templateCard).instructions("query")))
    } else None
  }

  val templateCard: String = "describe %i:query"

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "describe"

}