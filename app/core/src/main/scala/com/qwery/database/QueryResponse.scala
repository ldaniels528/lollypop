package com.qwery.database

import com.qwery.AppConstants._
import com.qwery.runtime.ModelsJsonProtocol._
import com.qwery.runtime.QweryVM.convertToTable
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo.{ProductToRowCollection, createQueryResultTable}
import com.qwery.runtime.devices.{RowCollection, TableColumn}
import com.qwery.runtime.instructions.expressions.GraphResult
import com.qwery.runtime.instructions.queryables.TableRendering
import com.qwery.runtime.{DataObject, DatabaseObjectNS, ROWID, SRC_ROWID_NAME, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.{IOCost, RowIDRange}
import spray.json.RootJsonFormat

/**
 * Represents the results of a query
 * @param ns         the [[DatabaseObjectNS object namespace]]
 * @param resultType the result type (e.g. "CHART")
 * @param cost       the optional [[IOCost cost]]
 * @param columns    the table/drawing [[TableColumn columns]]
 * @param rows       the table/drawing data rows
 * @param options    the chart options
 * @param stdErr     the server-side STDERR output
 * @param stdOut     the server-side STDOUT output
 */
case class QueryResponse(ns: DatabaseObjectNS,
                         resultType: String,
                         cost: Option[IOCost] = None,
                         columns: Seq[TableColumn] = Nil,
                         rows: Seq[Seq[Any]] = Nil,
                         options: Option[Map[String, Any]] = None,
                         stdErr: String,
                         stdOut: String) extends DataObject {

  def foreach(f: Map[String, Any] => Unit): Unit = {
    val columnNames = columns.map(_.name)
    rows foreach { values => f(Map(columnNames zip values: _*)) }
  }

  def get: Either[RowCollection, IOCost] = {
    // is it a row collection?
    if (columns.isEmpty) Right(cost || IOCost.empty) else {
      val out = createQueryResultTable(columns)
      for {row <- rows} {
        val mapping = Map(columns.map(_.name) zip row: _*)
        out.insert(mapping.toRow(out))
      }
      Left(out)
    }
  }

  def getUpdateCount: Int = cost.map(_.getUpdateCount) || 0

  def __ids: List[ROWID] = cost.toList.flatMap(_.rowIDs.toList)

  def tabulate(limit: Int = Int.MaxValue): List[String] = this.toRowCollection.tabulate(limit)

}

/**
 * QueryResult Companion
 */
object QueryResponse {
  val RESULT_DRAWING = "CHART"
  val RESULT_ROWS = "ROWS"

  def getSource(ns_? : Option[DatabaseObjectNS])(implicit scope: Scope): DatabaseObjectNS = {
    ns_? || DatabaseObjectNS(
      databaseName = scope.getDatabase || DEFAULT_DATABASE,
      schemaName = scope.getSchema || DEFAULT_SCHEMA,
      name = "???"
    )
  }

  final implicit val queryResultJsonFormat: RootJsonFormat[QueryResponse] = jsonFormat8(QueryResponse.apply)

  final implicit class QueryResultConversion(val result: Any) extends AnyVal {

    def toQueryResponse(ref: Option[DatabaseObjectNS], limit: Option[Int])(implicit scope: Scope): QueryResponse = {
      val ns = getSource(ref)
      result match {
        case cost: IOCost => toQueryResponse(ns, cost)
        case rc: RowCollection => toQueryResponse(ns, rc, limit)
        case drawing: GraphResult => toQueryResponse(ns, rc = drawing.data, limit = None).copy(resultType = RESULT_DRAWING)
        case tr: TableRendering => toQueryResponse(ns, tr.toTable, limit)
        case _ids: Seq[_] if _ids.forall(_.isInstanceOf[ROWID]) =>
          val rowIds = _ids.collect { case n: ROWID => n }
          toQueryResponse(ns, IOCost(inserted = rowIds.size, rowIDs = RowIDRange(rowIds: _*)))
        case value => toQueryResponse(ns, convertToTable(columnName = "result", value), limit)
      }
    }

    private def toQueryResponse(ns: DatabaseObjectNS, cost: IOCost)(implicit scope: Scope): QueryResponse = {
      val device = cost.toRowCollection
      val columns = device.columns
      QueryResponse(
        resultType = RESULT_ROWS,
        ns = ns,
        columns = columns,
        rows = device.toList map { row =>
          val mapping = row.toMap
          columns.map(_.name).map(mapping.get)
        },
        cost = Option(cost),
        stdOut = scope.getUniverse.system.stdOut.asString(),
        stdErr = scope.getUniverse.system.stdErr.asString())
    }

    private def toQueryResponse(ns: DatabaseObjectNS, rc: RowCollection, limit: Option[Int])(implicit scope: Scope): QueryResponse = {
      val rows = limit.map(n => rc.toList.take(n)) || rc.toList
      val columns = rc.columns
      QueryResponse(
        resultType = RESULT_ROWS,
        ns = ns,
        columns = columns,
        rows = rows map { row =>
          val mapping = row.toMap
          columns.map(_.name).map(mapping.get(_).orNull)
        },
        cost = Some(IOCost(rowIDs = RowIDRange(rows.map { row => row.getField(SRC_ROWID_NAME).flatMap(_.value).collect { case id: Long => id } || row.id }))),
        stdOut = scope.getUniverse.system.stdOut.asString(),
        stdErr = scope.getUniverse.system.stdErr.asString())
    }

  }

}