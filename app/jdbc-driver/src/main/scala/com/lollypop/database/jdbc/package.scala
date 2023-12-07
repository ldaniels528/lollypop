package com.lollypop.database

import com.lollypop.database.jdbc.types.JDBCValueConversion.toDataType
import com.lollypop.language._
import com.lollypop.runtime._
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.util.StringRenderHelper.StringRenderer
import com.lollypop.util.Tabulator

import java.sql.{ResultSet, SQLException}

package object jdbc {

  def die(message: String): Nothing = throw new SQLException(message)

  def dieParameterOutOfRange(index: Int): Nothing = throw new SQLException(s"Parameter index ($index) is out of range")

  def dieParameterNotFound(name: String): Nothing = throw new SQLException(s"Parameter ($name) not found")

  def dieUnsupportedConversion(x: Any, typeName: String): Nothing = {
    die(s"Conversion from '${x.renderAsJson.limit(30)}' (${x.getClass.getName}) to $typeName is not supported")
  }

  final implicit class RichResult(val rs: ResultSet) extends AnyVal {
    @inline
    def getColumns: List[String] = {
      val rsmd = rs.getMetaData
      (for (n <- 1 to rsmd.getColumnCount) yield rsmd.getColumnName(n)).toList
    }

    @inline
    def tabulate(limit: Int = Int.MaxValue): List[String] = {
      Tabulator.tabulate(headers = rs.getColumns, rows = rs.toList.map(_.map {
        case o: Option[_] => o
        case v => Option(v)
      }), limit = limit)
    }

    @inline
    def toList: List[List[Any]] = {
      val columns = rs.getColumns

      def getRow: List[AnyRef] = for {name <- columns} yield rs.getObject(name)

      var list: List[List[Any]] = Nil
      while (rs.next()) {
        list = getRow :: list
      }
      list.reverse
    }

    @inline
    def toRowCollection: RowCollection = {
      val rsmd = rs.getMetaData
      val columns = (for (n <- 1 to rsmd.getColumnCount) yield {
        val name = rsmd.getColumnName(n)
        val `type` = toDataType(rsmd.getColumnType(n))
        TableColumn(name, `type`)
      }).toList
      val names = columns.map(_.name)
      val out = createQueryResultTable(columns)
      rs.toList foreach { values =>
        val keyValues = (names zip values).collect { case (k, v) if v != null => (k, v) }
        out.insert(Map(keyValues: _*).toRow(out))
      }
      out
    }

  }

}
