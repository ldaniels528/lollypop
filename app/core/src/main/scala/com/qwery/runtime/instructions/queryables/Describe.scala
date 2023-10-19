package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language._
import com.qwery.language.models.Instruction
import com.qwery.runtime.datatypes.{BooleanType, StringType, TableType}
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo._
import com.qwery.runtime.devices.{RowCollection, TableColumn}
import com.qwery.runtime.instructions.expressions.TableExpression
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, Scope}
import qwery.io.IOCost

/**
 * SQL describe `query`
 * @param queryable the [[Instruction query]] to describe
 */
case class Describe(queryable: Instruction) extends RuntimeQueryable with TableExpression {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scope1, cost1, result1) = QweryVM.search(scope, queryable)
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
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a table representing the layout of the query expression",
    example = "describe (select v1: 123, v2: 'abc')"
  ))

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Describe = {
    Describe(SQLTemplateParams(ts, templateCard).instructions("query"))
  }

  val templateCard: String = "describe %i:query"

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "describe"

}