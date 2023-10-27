package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_TRANSFORMATION, PARADIGM_DECLARATIVE}
import com.qwery.language._
import com.qwery.language.models.{Condition, Expression, FieldRef, Queryable}
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.{Row, RowCollection}
import com.qwery.runtime.instructions.ReferenceInstruction
import com.qwery.runtime.instructions.queryables.AssumeQueryable.EnrichedAssumeQueryable
import com.qwery.runtime.instructions.queryables.RowsOfValues
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, Scope}
import qwery.io.IOCost

/**
 * Inserts (or updates) a row into a table
 * @param ref       the [[DatabaseObjectRef reference]] to the table to update
 * @param fields    the given collection of [[FieldRef field references]]
 * @param source    the given [[Queryable source]]
 * @param condition the [[Condition where clause]]
 * @example
 * {{{
 * upsert into stocks (exchange, transactions)
 * values ('OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
 *                   {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}])
 * where symbol is 'AMD'
 * }}}
 */
case class UpsertInto(ref: DatabaseObjectRef,
                      fields: Seq[FieldRef] = Nil,
                      source: Queryable,
                      condition: Condition)
  extends RuntimeModifiable with ReferenceInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val rc = scope.getRowCollection(ref)
    val cost = source match {
      case RowsOfValues(rowValues) => rc.upsert(row = extractRow(rc, rowValues), condition)
      case queryable =>
        //        val (scope1, cost1, result1) = DatabaseVM.search(scope, queryable)
        //        if (result1.isEmpty) cost1
        //        else if (result1.getLength > 1) die("Upserting multiple rows is not supported")
        //        else {
        //          val rowID_? = result1.indexWhereMetadata()(_.isActive)
        //          val row = rowID_?.map(result1.apply).orNull
        //          cost1 ++ rc.upsert(row, condition)(scope1)
        //        }
        dieIllegalType(queryable)
    }
    (scope, cost, cost)
  }

  override def toSQL: String = {
    (List("upsert into", ref.toSQL, fields.map(_.toSQL).mkString("(", ", ", ")"), source.toSQL)
      ::: "where" :: condition.toSQL :: Nil).mkString(" ")
  }

  private def extractRow(rc: RowCollection, rowValues: Seq[Seq[Expression]])(implicit scope: Scope): Row = {
    if (rowValues.size > 1) die("Upserting multiple rows is not supported")
    val fieldNames = if (fields.nonEmpty) fields.map(_.name) else rc.columns.map(_.name)
    val values = rowValues.headOption.toList.flatMap(_.map(QweryVM.execute(scope, _)._3))
    if (fieldNames.size != values.size) die(s"Field values mismatch: ${fieldNames.size} field(s), but ${values.size} value(s)")
    Map((fieldNames zip values).map { case (name, value) => name -> value }: _*).toRow(rc)
  }

}

object UpsertInto extends ModifiableParser with InsertValues {
  private val template = "upsert into %L:target ?( +?%F:fields +?) %V:source where %c:condition"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "upsert",
    category = CATEGORY_TRANSFORMATION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Inserts (or updates) new row(s) into a table",
    isExperimental = true,
    example =
      """|namespace "temp.examples"
         |drop if exists Stocks &&
         |create table Stocks (symbol: String(8), exchange: String(8), lastSale: Double) &&
         |create index Stocks#symbol &&
         |insert into Stocks (symbol, exchange, lastSale)
         |  |------------------------------|
         |  | symbol | exchange | lastSale |
         |  |------------------------------|
         |  | ATT    | NYSE     |    66.78 |
         |  | UPEX   | NASDAQ   |   116.24 |
         |  | XYZ    | AMEX     |    31.95 |
         |  | ABC    | OTCBB    |    5.887 |
         |  |------------------------------|
         |upsert into Stocks (symbol, exchange, lastSale) values ('AAPL', 'NASDAQ', 156.39) where symbol is 'AAPL'
         |ns('Stocks')
         |""".stripMargin
  ), HelpDoc(
    name = "upsert",
    category = CATEGORY_TRANSFORMATION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Inserts (or updates) new row(s) into a table",
    isExperimental = true,
    example =
      """|namespace "temp.examples"
         |drop if exists Stocks &&
         |create table Stocks (symbol: String(8), exchange: String(8), lastSale: Double) &&
         |create index Stocks#symbol &&
         |insert into Stocks (symbol, exchange, lastSale)
         |  |------------------------------|
         |  | symbol | exchange | lastSale |
         |  |------------------------------|
         |  | AAPL   | NASDAQ   |   156.12 |
         |  | ATT    | NYSE     |    66.78 |
         |  | UPEX   | NASDAQ   |   116.24 |
         |  | XYZ    | AMEX     |    31.95 |
         |  | ABC    | OTCBB    |    5.887 |
         |  |------------------------------|
         |upsert into Stocks (symbol, exchange, lastSale) values ('AAPL', 'NASDAQ', 156.39) where symbol is 'AAPL'
         |ns('Stocks')
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): UpsertInto = {
    val params = SQLTemplateParams(ts, template)
    UpsertInto(
      ref = params.locations("target"),
      source = params.instructions("source").asQueryable,
      fields = params.fieldLists.getOrElse("fields", Nil),
      condition = params.conditions("condition"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "upsert into"

}