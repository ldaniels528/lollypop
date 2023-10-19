package com.qwery.database.jdbc.types

import java.util

case class JDBCRef(var value: Any) extends java.sql.Ref {
  override def getBaseTypeName: String = Option(value).map(_.getClass.getName).orNull

  override def getObject(map: util.Map[String, Class[_]]): Any = value

  override def getObject: Any = value

  override def setObject(value: Any): Unit = this.value = value

}
