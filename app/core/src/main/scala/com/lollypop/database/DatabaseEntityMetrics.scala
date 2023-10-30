package com.lollypop.database

import com.lollypop.runtime.devices.TableColumn
import com.lollypop.runtime.{DatabaseObjectNS, ROWID}
import spray.json.RootJsonFormat

case class DatabaseEntityMetrics(ns: DatabaseObjectNS,
                                 columns: Seq[TableColumn],
                                 physicalSize: Option[Long],
                                 recordSize: Int,
                                 rows: ROWID)

object DatabaseEntityMetrics {
  import com.lollypop.runtime.ModelsJsonProtocol._

  final implicit val tableMetricsJsonFormat: RootJsonFormat[DatabaseEntityMetrics] = jsonFormat5(DatabaseEntityMetrics.apply)

}