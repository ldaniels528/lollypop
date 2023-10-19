package com.qwery.database

import com.qwery.runtime.devices.TableColumn
import com.qwery.runtime.{DatabaseObjectNS, ROWID}
import spray.json.RootJsonFormat

case class DatabaseEntityMetrics(ns: DatabaseObjectNS,
                                 columns: Seq[TableColumn],
                                 physicalSize: Option[Long],
                                 recordSize: Int,
                                 rows: ROWID)

object DatabaseEntityMetrics {
  import com.qwery.runtime.ModelsJsonProtocol._

  final implicit val tableMetricsJsonFormat: RootJsonFormat[DatabaseEntityMetrics] = jsonFormat5(DatabaseEntityMetrics.apply)

}