package com.lollypop.database
package jdbc

import com.lollypop.database.jdbc.types.JDBCWrapper
import com.lollypop.runtime.datatypes.Inferences

/**
 * Lollypop JDBC Parameter Metadata
 */
class JDBCParameterMetaData() extends java.sql.ParameterMetaData with JDBCWrapper {
  private val parameters = new JDBCParameters()

  def apply(parameterNumber: Int): Any = parameters(parameterNumber)

  def clear(): Unit = parameters.clear()

  override def getParameterCount: Int = parameters.size

  def getParameters: List[Any] = parameters.toList

  override def getPrecision(parameterNumber: Int): Int = {
    val value = parameters(parameterNumber)
    val columnType = Inferences.fromValue(value)
    columnType.maxSizeInBytes
  }

  override def getScale(parameterNumber: Int): Int = {
    parameters(parameterNumber) match {
      case b: BigDecimal => b.scale
      case n: Number => BigDecimal(n.doubleValue()).scale
      case _ => 0
    }
  }

  override def getParameterType(parameterNumber: Int): Int = Inferences.fromValue(parameters(parameterNumber)).getJDBCType

  override def getParameterTypeName(parameterNumber: Int): String = Inferences.fromValue(parameters(parameterNumber)).name

  override def getParameterClassName(parameterNumber: Int): String = Inferences.fromValue(parameters(parameterNumber)).toJavaType(true).getName

  override def getParameterMode(parameterNumber: Int): Int = {
    assert(parameterNumber <= parameters.size, die(s"Parameter index out of bounds ($parameterNumber)"))
    java.sql.ParameterMetaData.parameterModeIn
  }

  override def isNullable(parameterNumber: Int): Int = {
    if (parameters.isNullable(parameterNumber)) java.sql.ParameterMetaData.parameterNullable else java.sql.ParameterMetaData.parameterNoNulls
  }

  override def isSigned(parameterNumber: Int): Boolean = Inferences.fromValue(parameters(parameterNumber)).isSigned

  def update(parameterNumber: Int, value: Any): Unit = {
    parameters(parameterNumber) = value
  }

}
