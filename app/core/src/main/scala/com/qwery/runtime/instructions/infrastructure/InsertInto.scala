package com.qwery.runtime.instructions.infrastructure

import com.qwery.implicits.MagicImplicits
import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language._
import com.qwery.language.models.{Condition, Expression, FieldRef, Queryable}
import com.qwery.runtime.devices.RowCollectionZoo.RichDatabaseObjectRef
import com.qwery.runtime.instructions.ReferenceInstruction
import com.qwery.runtime.instructions.queryables.AssumeQueryable.EnrichedAssumeQueryable
import com.qwery.runtime.instructions.queryables.RowsOfValues
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, Scope}
import qwery.io.{IOCost, RowIDRange}

/**
 * Inserts rows into a table
 * @param ref       the [[DatabaseObjectRef reference]] to the table to update
 * @param source    the given [[Queryable source]]
 * @param fields    the given collection of [[FieldRef field references]]
 * @param condition the optional [[Condition where clause]]
 * @param limit     the optional [[Expression limit]]
 * @example
 * {{{
 * insert into stocks (symbol, exchange, transactions)
 * values ('AAPL', 'NASDAQ', {"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}),
 *        ('AMD', 'NASDAQ',  {"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}),
 *        ('INTC','NYSE',    {"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}),
 *        ('AMZN', 'NASDAQ', {"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}),
 *        ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
 *                          {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}])
 * }}}
 * @example
 * {{{
 * insert into stocks#transactions (price, transactionTime)
 * values (35.11, "2021-08-05T19:23:12.000Z"),
 *        (35.83, "2021-08-05T19:23:15.000Z"),
 *        (36.03, "2021-08-05T19:23:17.000Z")
 * where symbol is 'AMD'
 * }}}
 */
case class InsertInto(ref: DatabaseObjectRef,
                      source: Queryable,
                      fields: Seq[FieldRef] = Nil,
                      condition: Option[Condition] = None,
                      limit: Option[Expression] = None)
  extends RuntimeModifiable with ReferenceInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = ref match {
      // is it an inner table reference? (e.g. 'stocks#transactions')
      case st: DatabaseObjectRef if st.isSubTable =>
        assert(fields.nonEmpty, die(s"Fields must be specified for inner table '$ref'"))
        source match {
          case RowsOfValues(rowValues) =>
            st.inside { case (outerTable, innerTableColumn) =>
              outerTable.insertInside(innerTableColumn, injectColumns = fields.map(_.name), rowValues, condition, limit)
            }
          case other => dieUnsupportedType(other)
        }
      // must be a standard table reference (e.g. 'stocks')
      case _ =>
        assert(condition.isEmpty, die("where is only supported for inner-tables"))
        source match {
          case RowsOfValues(values) =>
            val dest = scope.getRowCollection(ref)
            val destFieldNames = if (fields.nonEmpty) fields.map(_.name) else dest.columns.map(_.name)
            dest.insertRows(destFieldNames, values)
          case queryable =>
            val (scope1, cost1, result1) = QweryVM.search(scope, queryable)
            val device = scope1.getRowCollection(ref)
            val start = device.getLength
            val rowIDs = (start until start + result1.getLength).toList
            device.insert(result1) ~> { io => cost1 ++ io.copy(rowIDs = RowIDRange(io.rowIDs.toList ::: rowIDs)) }
        }
    }
    (scope, cost, cost)
  }

  override def toSQL: String = {
    (List("insert into", ref.toSQL, fields.map(_.toSQL).mkString("(", ", ", ")"), source.toSQL)
      ::: condition.map(i => s"where ${i.toSQL}").toList
      ::: limit.map(i => s"limit ${i.toSQL}").toList
      ).mkString(" ")
  }

}

object InsertInto extends ModifiableParser with InsertValues {
  val template: String = "insert into %L:target ?( +?%F:fields +?) %V:source ?where +?%c:condition ?limit +?%e:limit"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "insert",
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Appends new row(s) to a table",
    example =
      """|stagedActors =
         ||------------------------------------------|
         || name              | popularity | ratio   |
         ||------------------------------------------|
         || John Wayne        | 42         |  0.4206 |
         || Carry Grant       | 87         |  0.8712 |
         || Doris Day         | 89         |  0.8907 |
         || Audrey Hepburn    | 97         |  0.9732 |
         || Gretta Garbeaux   | 57         |  0.5679 |
         ||------------------------------------------|
         |
         |declare table Actors (name: String(64), popularity: Int)
         |insert into Actors (name, popularity) select name, popularity from @@stagedActors
         |
         |graph { shape: "bar", title: "Popularity" } from Actors
         |""".stripMargin
  ), HelpDoc(
    name = "insert",
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Appends new row(s) to a table",
    example =
      """|declare table Stocks(symbol: String(8), exchange: String(8), transactions: Table(price: Double, transactionTime: DateTime))
         |insert into Stocks (symbol, exchange, transactions)
         |values ('AAPL', 'NASDAQ', {"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}),
         |       ('AMD', 'NASDAQ',  {"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}),
         |       ('INTC','NYSE',    {"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}),
         |       ('AMZN', 'NASDAQ', {"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}),
         |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
         |                          {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}])
         |
         |insert into Stocks#transactions (price, transactionTime)
         |values (35.11, "2021-08-05T19:23:12.000Z"),
         |       (35.83, "2021-08-05T19:23:15.000Z"),
         |       (36.03, "2021-08-05T19:23:17.000Z")
         |where symbol is 'AMD'
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): InsertInto = {
    val params = SQLTemplateParams(ts, template)
    InsertInto(
      ref = params.locations("target"),
      source = params.instructions("source").asQueryable,
      fields = params.fieldLists.getOrElse("fields", Nil),
      condition = params.conditions.get("condition"),
      limit = params.expressions.get("limit"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "insert into"

}