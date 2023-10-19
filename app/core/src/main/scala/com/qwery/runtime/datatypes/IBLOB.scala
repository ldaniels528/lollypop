package com.qwery.runtime.datatypes

import com.qwery.runtime.devices.RowCollectionZoo.createTempNS
import com.qwery.runtime.{DataObject, DatabaseObjectNS, QweryNative}

import java.io.{InputStream, OutputStream}
import java.sql.Blob

/**
 * Represents a Binary Large Object (BLOB)
 */
trait IBLOB extends java.sql.Blob with DataObject with QweryNative {

  override def returnType: DataType = BlobType

}

/**
 * IBLOB Companion
 */
object IBLOB {

  def apply(blob: java.sql.Blob): IBLOB = apply(dns = createTempNS(), blob)

  def apply(blob: IBLOB): IBLOB = apply(dns = blob.ns, blob)

  def apply(dns: DatabaseObjectNS, blob: java.sql.Blob): IBLOB = new IBLOB {
    override def ns: DatabaseObjectNS = dns

    override def equals(obj: Any): Boolean = blob.equals(obj)

    override def hashCode(): Int = blob.hashCode()

    override def length(): Long = blob.length()

    override def getBytes(pos: Long, length: Int): Array[Byte] = blob.getBytes(pos, length)

    override def getBinaryStream: InputStream = blob.getBinaryStream

    override def position(pattern: Array[Byte], start: Long): Long = blob.position(pattern, start)

    override def position(pattern: Blob, start: Long): Long = blob.position(pattern, start)

    override def setBytes(pos: Long, bytes: Array[Byte]): Int = blob.setBytes(pos, bytes)

    override def setBytes(pos: Long, bytes: Array[Byte], offset: Int, len: Int): Int = blob.setBytes(pos, bytes, offset, len)

    override def setBinaryStream(pos: Long): OutputStream = blob.setBinaryStream(pos)

    override def truncate(len: Long): Unit = blob.truncate(len)

    override def free(): Unit = blob.free()

    override def getBinaryStream(pos: Long, length: Long): InputStream = blob.getBinaryStream(pos, length)
  }

}