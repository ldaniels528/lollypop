package com.qwery.database.jdbc.types

import com.qwery.runtime.datatypes.DataType

case class JDBCStruct(dataType: DataType, attributes: Array[AnyRef]) extends java.sql.Struct {

  override def getSQLTypeName: String = dataType.toSQL

  override def getAttributes: Array[AnyRef] = attributes

  override def getAttributes(map: java.util.Map[String, Class[_]]): Array[AnyRef] = attributes // TODO do something about this some day

}
