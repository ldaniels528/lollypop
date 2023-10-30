package com.lollypop.database
package jdbc

import com.lollypop.database.jdbc.types.JDBCWrapper
import com.lollypop.runtime.DatabaseObjectNS
import com.lollypop.runtime.devices.TableColumn

import java.sql.ResultSetMetaData

/**
 * Lollypop Result Set Metadata
 * @param ns      the [[DatabaseObjectNS realized database object namespace]]
 * @param columns the collection of [[TableColumn]]
 */
class JDBCResultSetMetaData(ns: DatabaseObjectNS, columns: Seq[TableColumn]) extends ResultSetMetaData with JDBCWrapper {

  override def getColumnCount: Int = columns.length

  override def isAutoIncrement(column: Int): Boolean = columns(checkColumnIndex(column)).`type`.isAutoIncrement

  override def isCaseSensitive(column: Int): Boolean = {
    checkColumnIndex(column)
    true
  }

  override def isCurrency(column: Int): Boolean = {
    checkColumnIndex(column)
    false
  }

  override def isNullable(column: Int): Int = {
    if (columns(checkColumnIndex(column)).defaultValue.isEmpty) 1 else 2
  }

  override def isSearchable(column: Int): Boolean = {
    checkColumnIndex(column)
    true
  }

  override def isSigned(column: Int): Boolean = columns(checkColumnIndex(column)).`type`.isSigned

  override def getColumnDisplaySize(column: Int): Int = columns(checkColumnIndex(column)).`type`.maxSizeInBytes

  override def getColumnLabel(column: Int): String = columns(checkColumnIndex(column)).name

  override def getColumnName(column: Int): String = columns(checkColumnIndex(column)).name

  override def getPrecision(column: Int): Int = columns(checkColumnIndex(column)).`type`.precision

  override def getScale(column: Int): Int = columns(checkColumnIndex(column)).`type`.scale

  override def getSchemaName(column: Int): String = {
    checkColumnIndex(column)
    ns.schemaName
  }

  override def getTableName(column: Int): String = {
    checkColumnIndex(column)
    ns.name
  }

  override def getCatalogName(column: Int): String = {
    checkColumnIndex(column)
    ns.databaseName
  }

  override def getColumnType(column: Int): Int = columns(checkColumnIndex(column)).`type`.getJDBCType

  override def getColumnTypeName(column: Int): String = columns(checkColumnIndex(column)).`type`.name

  override def isReadOnly(column: Int): Boolean = {
    checkColumnIndex(column)
    false
  }

  override def isWritable(column: Int): Boolean = {
    checkColumnIndex(column)
    true
  }

  override def isDefinitelyWritable(column: Int): Boolean = {
    checkColumnIndex(column)
    true
  }

  override def getColumnClassName(column: Int): String = columns(checkColumnIndex(column)).`type`.toString

  private def checkColumnIndex(column: Int): Int = {
    assert(column > 0 && column <= columns.length, die(s"Column index ($column) out of range"))
    column - 1
  }

}
