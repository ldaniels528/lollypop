package com.qwery.util

import com.qwery.implicits.MagicImplicits
import com.qwery.runtime.ROWID
import com.qwery.runtime.devices.{FieldMetadata, RowMetadata}

import java.nio.{Buffer, ByteBuffer}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/**
 * ByteBuffer Helper
 */
object ByteBufferHelper {

  /**
   * Data Type Buffer
   * @param buf the given [[Buffer]]
   */
  final implicit class DataTypeBuffer[A <: Buffer](val buf: A) extends AnyVal {
    def flipMe(): buf.type = {
      buf.flip()
      buf
    }
  }

  /**
   * Data Type Byte Buffer
   * @param buf the given [[ByteBuffer byte buffer]]
   */
  final implicit class DataTypeByteBuffer(val buf: ByteBuffer) extends AnyVal {

    def getBoolean: Boolean = buf.get ~> { n => n != 0 }

    def putBoolean(state: Boolean): ByteBuffer = buf.put((if (state) 1 else 0).toByte)

    def getBytes: Array[Byte] = {
      val bytes = new Array[Byte](getLength32)
      buf.get(bytes)
      bytes
    }

    def putBytes(bytes: Array[Byte]): ByteBuffer = buf.putInt(bytes.length).put(bytes)

    def getDate: java.util.Date = new java.util.Date(buf.getLong)

    def putDate(date: java.util.Date): ByteBuffer = buf.putLong(date.getTime)

    def getFieldMetadata: FieldMetadata = FieldMetadata.decode(buf.get)

    def putFieldMetadata(fmd: FieldMetadata): ByteBuffer = buf.put(fmd.encode)

    def getInterval: FiniteDuration = (getLength64, getLength32) ~> { case (length, unit) => new FiniteDuration(length, TimeUnit.values()(unit)) }

    def putInterval(fd: FiniteDuration): ByteBuffer = buf.putLong(fd.length).putInt(fd.unit.ordinal())

    def getRowID: ROWID = buf.getLong

    def putRowID(rowID: ROWID): ByteBuffer = buf.putLong(rowID)

    def getRowMetadata: RowMetadata = RowMetadata.decode(buf.get)

    def putRowMetadata(rmd: RowMetadata): ByteBuffer = buf.put(rmd.encode)

    def getLength32: Int = {
      val length = buf.getInt
      assert(length >= 0, "Length must be positive")
      length
    }

    def getLength64: Long = {
      val length = buf.getLong
      assert(length >= 0, "Length must be positive")
      length
    }

    def getText: String = {
      val bytes = new Array[Byte](getLength32)
      buf.get(bytes)
      new String(bytes)
    }

    def getUUID: UUID = new UUID(buf.getLong, buf.getLong)

    def putUUID(uuid: UUID): ByteBuffer = buf.putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits)

  }

}
