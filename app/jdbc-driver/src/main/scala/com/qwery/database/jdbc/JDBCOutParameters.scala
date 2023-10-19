package com.qwery.database.jdbc

import java.sql.ResultSet
import scala.collection.mutable

/**
 * JDBC Out Parameters
 */
class JDBCOutParameters() {
  private val parametersByIndex = mutable.LinkedHashMap[Int, Any]()
  private val parametersByName = mutable.LinkedHashMap[String, Any]()
  private val registeredByIndex = mutable.LinkedHashMap[Int, Int]()
  private val registeredByName = mutable.LinkedHashMap[String, Int]()

  def apply(parameterIndex: Int): Any = {
    parametersByIndex.getOrElse(parameterIndex, dieParameterOutOfRange(parameterIndex))
  }

  def apply(parameterName: String): Any = {
    parametersByName.getOrElse(parameterName, dieParameterNotFound(parameterName))
  }

  def register(parameterIndex: Int, sqlType: Int): Unit = {
    registeredByIndex(parameterIndex) = sqlType
  }

  def register(parameterName: String, sqlType: Int): Unit = {
    registeredByName(parameterName) = sqlType
  }

  def update(parameterIndex: Int, value: Any): Unit = {
    parametersByIndex(parameterIndex) = value
  }

  def update(parameterName: String, value: Any): Unit = {
    parametersByName(parameterName) = value
  }

  def updateResults(resultSet: ResultSet): Unit = {
    parametersByIndex.clear()
    parametersByName.clear()

    // update the parameters by name and index
    if (resultSet.next()) {
      val metaData = resultSet.getMetaData
      for {
        paramIndex <- 1 to metaData.getColumnCount
        paramName = metaData.getColumnName(paramIndex)
        value = resultSet.getObject(paramIndex) if registeredByName.contains(paramName) || registeredByIndex.contains(paramIndex)
      } {
        parametersByName(paramName) = value
        parametersByIndex(paramIndex) = value
      }
    }

    // reset the result set
    resultSet.beforeFirst()
  }
}