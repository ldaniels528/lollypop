package com.lollypop.runtime.datatypes

import com.lollypop.runtime.DatabaseObjectNS
import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import com.lollypop.util.IOTools.{RichInputStream, RichReader}
import com.lollypop.util.ResourceHelper._
import org.apache.commons.io.IOUtils

import java.io._

/**
 * Lollypop-native JDBC-compatible CLOB implementation
 * @param ns  the [[DatabaseObjectNS namespace]]
 * @param raf the [[RandomAccessFile random access file]]
 * @example {{{
 *   CLOB('Hello World')
 * }}}
 */
class CLOB(val ns: DatabaseObjectNS, val raf: RandomAccessFile) extends ICLOB with AbstractLOB {

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: java.sql.Clob => this.getAsciiStream.mkString() == that.getAsciiStream.mkString()
      case _ => super.equals(obj)
    }
  }

  override def hashCode(): Int = toString.hashCode()

  override def getAsciiStream: InputStream = createInputStream()

  override def getCharacterStream: Reader = new InputStreamReader(getAsciiStream)

  override def getCharacterStream(pos: Long, length: Long): Reader = new InputStreamReader(createInputStream(pos, length))

  override def getSubString(pos: Long, length: Int): String = {
    raf.seek(pos)
    val buf = new Array[Byte](length)
    raf.readFully(buf)
    new String(buf)
  }

  override def position(searchString: String, start: Long): Long = {
    val pattern = searchString.getBytes()
    var pos: Long = start
    val buf = new Array[Byte](pattern.length)

    def matchesPattern(): Boolean = {
      raf.seek(pos)
      raf.readFully(buf)
      (buf zip pattern).forall { case (a, b) => a == b }
    }

    raf.seek(start)
    var matched = false
    while (!matched && (pos + pattern.length < raf.length())) {
      matched = matchesPattern()
      pos += 1
    }
    if (matched) pos else -1L
  }

  override def position(searchString: java.sql.Clob, start: Long): Long = {
    position(searchString.getSubString(0, searchString.length().toInt), start)
  }

  override def setAsciiStream(pos: Long): OutputStream = createOutputStream(pos)

  override def setCharacterStream(pos: Long): Writer = new OutputStreamWriter(setAsciiStream(pos))

  override def setString(pos: Long, string: String): Int = {
    raf.seek(pos)
    raf.write(string.getBytes())
    string.length
  }

  override def setString(pos: Long, string: String, offset: Int, len: Int): Int = {
    val subString = string.substring(offset, offset + len)
    raf.seek(pos)
    raf.write(subString.getBytes())
    subString.length
  }

  override def toString: String = s"${getClass.getSimpleName}(\"${getCharacterStream.mkString()}\")"

}

object CLOB {

  /**
   * Creates a new empty CLOB
   * @return a [[ICLOB CLOB]]
   */
  def apply(): ICLOB = {
    val ns = createTempNS()
    ns.createRoot()
    val clob: ICLOB = new CLOB(ns, new RandomAccessFile(ns.clobDataFile, "rw"))
    clob
  }

  /**
   * Creates a new CLOB from a [[String]] source
   * @param string the string source
   * @return a [[ICLOB CLOB]]
   */
  def apply(string: String): ICLOB = fromChars(string.toArray)

  /**
   * Creates a new CLOB from a byte array
   * @param bytes the byte array source
   * @return a [[ICLOB CLOB]]
   */
  def fromBytes(bytes: Array[Byte]): ICLOB = {
    val clob = CLOB()
    clob.setAsciiStream(0).use(_.write(bytes))
    clob
  }

  /**
   * Creates a new CLOB from a character array source
   * @param chars the character array source
   * @return a [[ICLOB CLOB]]
   */
  def fromChars(chars: Array[Char]): ICLOB = {
    val clob = CLOB()
    clob.setCharacterStream(0).use(_.write(chars))
    clob
  }

  /**
   * Creates a new CLOB from a [[File]] source
   * @param file the [[File file]] source
   * @return a [[ICLOB CLOB]]
   */
  def fromFile(file: File): ICLOB = new FileInputStream(file).use(fromInputStream)

  /**
   * Creates a new CLOB from a [[InputStream]] source
   * @param in the [[InputStream input stream]] source
   * @return a [[ICLOB CLOB]]
   */
  def fromInputStream(in: InputStream): ICLOB = {
    val clob = CLOB()
    clob.setAsciiStream(0).use(IOUtils.copy(in, _))
    clob
  }

  /**
   * Creates a new CLOB from a [[Reader]] source
   * @param reader the [[Reader reader]] source
   * @return a [[ICLOB CLOB]]
   */
  def fromReader(reader: Reader): ICLOB = {
    val clob = CLOB()
    clob.setCharacterStream(0).use(IOUtils.copy(reader, _))
    clob
  }

  /**
   * Creates a new CLOB from a [[Reader]] source
   * @param reader the [[fromReader reader]] source
   * @return a [[ICLOB CLOB]]
   */
  def fromReader(reader: Reader, length: Long): ICLOB = fromReader(reader.limitTo(length))

  /**
   * Creates a new CLOB from a [[String]] source
   * @param string the string source
   * @return a [[ICLOB CLOB]]
   */
  def fromString(string: String): ICLOB = fromChars(string.toArray)

}