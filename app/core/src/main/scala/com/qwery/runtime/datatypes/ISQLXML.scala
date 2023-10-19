package com.qwery.runtime.datatypes

import com.qwery.runtime.{DataObject, DatabaseObjectNS, QweryNative}
import com.qwery.runtime.devices.RowCollectionZoo.createTempNS

import java.io.{InputStream, OutputStream, Reader, Writer}
import javax.xml.transform.{Result, Source}

/**
 * Represents a SQL/XML Object (SQLXML)
 */
trait ISQLXML extends java.sql.SQLXML with DataObject with QweryNative {

  override def returnType: DataType = SQLXMLType

}

/**
 * ISQLXML Companion
 */
object ISQLXML {

  def apply(sqlXML: java.sql.SQLXML): ISQLXML = apply(dns = createTempNS(), sqlXML)

  def apply(sqlXML: ISQLXML): ISQLXML = apply(dns = sqlXML.ns, sqlXML)

  def apply(dns: DatabaseObjectNS, sqlXML: java.sql.SQLXML): ISQLXML = new ISQLXML {
    override def ns: DatabaseObjectNS = dns

    override def equals(obj: Any): Boolean = sqlXML.equals(obj)

    override def hashCode(): Int = sqlXML.hashCode()

    override def free(): Unit = sqlXML.free()

    override def getBinaryStream: InputStream = sqlXML.getBinaryStream

    override def setBinaryStream(): OutputStream = sqlXML.setBinaryStream()

    override def getCharacterStream: Reader = sqlXML.getCharacterStream

    override def setCharacterStream(): Writer = sqlXML.setCharacterStream()

    override def getString: String = sqlXML.getString

    override def setString(value: String): Unit = sqlXML.setString(value)

    override def getSource[T <: Source](sourceClass: Class[T]): T = sqlXML.getSource(sourceClass)

    override def setResult[T <: Result](resultClass: Class[T]): T = sqlXML.setResult(resultClass)
  }

}