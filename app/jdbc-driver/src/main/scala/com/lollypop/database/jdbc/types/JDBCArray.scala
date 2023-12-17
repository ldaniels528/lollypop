package com.lollypop.database.jdbc.types

import com.lollypop.database.jdbc.{JDBCConnection, JDBCResultSet, dieUnsupportedConversion}
import com.lollypop.language._
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.DataType
import com.lollypop.runtime.devices.TableColumn

import java.sql.ResultSet
import java.{sql, util}

/**
 * Represents a JDBC array
 * @param connection the [[JDBCConnection connection]]
 * @param typeName   the name of the item type
 * @param elements   the array elements
 */
case class JDBCArray(connection: JDBCConnection, typeName: String, elements: Array[_]) extends sql.Array {
  private val dataType = DataType.parse(typeName)(connection.scope)

  override def getBaseTypeName: String = dataType.name

  override def getBaseType: Int = dataType.getJDBCType

  override def getArray: AnyRef = elements

  override def getArray(map: util.Map[String, Class[_]]): AnyRef = elements

  override def getArray(index: Long, count: Int): Array[_] = {
    val n = index.toInt
    elements.slice(n, n + count)
  }

  override def getArray(index: Long, count: Int, map: util.Map[String, Class[_]]): Array[_] = {
    val n = index.toInt
    elements.slice(n, n + count)
  }

  override def getResultSet: ResultSet = createResultSet(start = 0, count = elements.length)

  override def getResultSet(map: util.Map[String, Class[_]]): ResultSet = createResultSet(start = 0, count = elements.length)

  override def getResultSet(index: Long, count: Int): ResultSet = createResultSet(index, count)

  override def getResultSet(index: Long, count: Int, map: util.Map[String, Class[_]]): ResultSet = createResultSet(index, count)

  override def free(): Unit = ()

  private def createResultSet(start: Long, count: Int): ResultSet = {
    new JDBCResultSet(
      connection = connection,
      ns = DatabaseObjectNS(
        databaseName = Option(connection.catalog) || DEFAULT_DATABASE,
        schemaName = Option(connection.schema) || DEFAULT_SCHEMA,
        name = java.lang.Long.toHexString(elements.hashCode())
      ),
      columns = Seq(TableColumn(name = "item", dataType)),
      rows = getArray(start, count).map(elem => Seq(elem)))
  }

}

object JDBCArray {

  def apply(connection: JDBCConnection, value: Any): sql.Array = {
    value match {
      case array: java.sql.Array => array
      case array: Array[Any] =>
        val componentType = array.getClass.getComponentType.getSimpleName
        new JDBCArray(connection, componentType, array)
      case x => dieUnsupportedConversion(x, "java.sql.Array")
    }
  }

}