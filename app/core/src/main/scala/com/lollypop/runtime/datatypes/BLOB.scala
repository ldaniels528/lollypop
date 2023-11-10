package com.lollypop.runtime.datatypes

import com.lollypop.runtime.DatabaseObjectNS
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import com.lollypop.util.IOTools.RichInputStream
import com.lollypop.util.ResourceHelper._
import org.apache.commons.io.IOUtils

import java.io._
import java.nio.charset.Charset

/**
 * Lollypop-native JDBC-compatible BLOB implementation
 * @param ns  the [[DatabaseObjectNS namespace]]
 * @param raf the [[RandomAccessFile random access file]]
 * @example {{{
 *   BLOB('Hello World')
 * }}}
 */
class BLOB(val ns: DatabaseObjectNS, val raf: RandomAccessFile) extends IBLOB with AbstractLOB {

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: java.sql.Blob => this.getBinaryStream.mkString() == that.getBinaryStream.mkString()
      case _ => super.equals(obj)
    }
  }

  override def hashCode(): Int = toString.hashCode()

  override def getBinaryStream: InputStream = getBinaryStream(pos = 0L, length())

  override def getBinaryStream(pos: Long, length: Long): InputStream = createInputStream(pos, length)

  override def getBytes(pos: Long, length: Int): Array[Byte] = {
    val buf = new Array[Byte](length)
    raf.seek(pos)
    raf.readFully(buf, 0, length)
    buf
  }

  override def position(pattern: java.sql.Blob, start: Long): Long = {
    position(pattern = pattern.getBytes(0, pattern.length().toInt), start)
  }

  override def position(pattern: Array[Byte], start: Long): Long = {
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

  override def setBinaryStream(pos: Long): OutputStream = createOutputStream(pos)

  override def setBytes(pos: Long, bytes: Array[Byte]): Int = {
    setBytes(pos, bytes, offset = 0, len = bytes.length)
  }

  override def setBytes(pos: Long, bytes: Array[Byte], offset: Int, len: Int): Int = {
    raf.seek(pos)
    raf.write(bytes, offset, len)
    len
  }

  override def toString: String = s"${getClass.getSimpleName}(\"${getBinaryStream.toBase64}\")"

}

object BLOB {

  /**
   * Creates a new empty BLOB
   * @return a [[IBLOB BLOB]]
   */
  def apply(): IBLOB = {
    val ns = createTempNS()
    ns.createRoot()
    val blob: IBLOB = new BLOB(ns, new RandomAccessFile(ns.tableDataFile, "rw"))
    blob
  }

  /**
   * Creates a new BLOB from a byte array
   * @param bytes the byte array source
   * @return a [[java.sql.Blob BLOB]]
   */
  def fromBytes(bytes: Array[Byte]): IBLOB = {
    val blob = BLOB()
    blob.setBinaryStream(0).use(_.write(bytes))
    blob
  }

  /**
   * Creates a new BLOB from a file source
   * @param file the [[File file]] source
   * @return a [[IBLOB BLOB]]
   */
  def fromFile(file: File): IBLOB = new FileInputStream(file).use(fromInputStream)

  /**
   * Creates a new BLOB from a file source
   * @param in the [[InputStream input stream]] source
   * @return a [[IBLOB BLOB]]
   */
  def fromInputStream(in: InputStream): IBLOB = {
    val blob = BLOB()
    blob.setBinaryStream(0).use(IOUtils.copy(in, _))
    blob
  }

  /**
   * Creates a new BLOB from a file source
   * @param in the [[InputStream input stream]] source
   * @return a [[IBLOB BLOB]]
   */
  def fromInputStream(in: InputStream, length: Long): IBLOB = fromInputStream(in.limitTo(length))

  /**
   * Creates a new BLOB from a [[Reader]] source
   * @param reader the [[Reader reader]] source
   * @return a [[IBLOB BLOB]]
   */
  def fromReader(reader: Reader): IBLOB = {
    val blob = BLOB()
    blob.setBinaryStream(0).use(IOUtils.copy(reader, _, Charset.defaultCharset()))
    blob
  }

  /**
   * Creates a new BLOB from a row collection
   * @param rc the [[RowCollection row collection]] source
   * @return a [[IIBLOB BLOB]]
   */
  def fromRowCollection(rc: RowCollection): IBLOB = {
    val ns = createTempNS(rc.columns)
    val blob: IBLOB = new BLOB(ns, new RandomAccessFile(ns.tableDataFile, "rw"))
    blob.setBinaryStream(0).write(rc.encode)
    blob
  }

  /**
   * Creates a new BLOB from a string source
   * @param string the [[String string]] source
   * @return a [[IBLOB BLOB]]
   */
  def fromString(string: String): IBLOB = fromBytes(string.getBytes())

}
