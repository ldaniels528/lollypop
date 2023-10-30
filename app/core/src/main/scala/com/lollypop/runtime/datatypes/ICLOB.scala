package com.lollypop.runtime.datatypes

import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import com.lollypop.runtime.{DataObject, DatabaseObjectNS, LollypopNative}

import java.io.{InputStream, OutputStream, Reader, Writer}
import java.sql.Clob

/**
 * Represents a Character Large Object (CLOB)
 */
trait ICLOB extends java.sql.NClob with DataObject with LollypopNative {

  override def returnType: DataType = ClobType

}

/**
 * ICLOB Companion
 */
object ICLOB {

  def apply(clob: java.sql.Clob): ICLOB = apply(dns = createTempNS(), clob)

  def apply(clob: ICLOB): ICLOB = apply(dns = clob.ns, clob)

  def apply(dns: DatabaseObjectNS, clob: java.sql.Clob): ICLOB = new ICLOB {
    override def ns: DatabaseObjectNS = dns

    override def equals(obj: Any): Boolean = clob.equals(obj)

    override def hashCode(): Int = clob.hashCode()

    override def length(): Long = clob.length()

    override def getSubString(pos: Long, length: Int): String = clob.getSubString(pos, length)

    override def getCharacterStream: Reader = clob.getCharacterStream

    override def getAsciiStream: InputStream = clob.getAsciiStream

    override def position(searchStr: String, start: Long): Long = clob.position(searchStr, start)

    override def position(searchClob: Clob, start: Long): Long = clob.position(searchClob, start)

    override def setString(pos: Long, str: String): Int = clob.setString(pos, str)

    override def setString(pos: Long, str: String, offset: Int, len: Int): Int = clob.setString(pos, str, offset, len)

    override def setAsciiStream(pos: Long): OutputStream = clob.setAsciiStream(pos)

    override def setCharacterStream(pos: Long): Writer = clob.setCharacterStream(pos)

    override def truncate(len: Long): Unit = clob.truncate(len)

    override def free(): Unit = clob.free()

    override def getCharacterStream(pos: Long, length: Long): Reader = clob.getCharacterStream(pos, length)
  }
}