package com.lollypop.util

import com.lollypop.runtime._
import org.apache.commons.io.IOUtils
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

/**
 * CODEC Helper
 */
object CodecHelper {

  def compressGZIP(bytes: Array[Byte]): Array[Byte] = {
    new ByteArrayOutputStream(bytes.length) use { baos =>
      new GZIPOutputStream(baos) use { gzos =>
        gzos.write(bytes)
        gzos.finish()
        gzos.flush()
      }
      baos.toByteArray
    }
  }

  def decompressGZIP(bytes: Array[Byte]): Array[Byte] = {
    new ByteArrayInputStream(bytes) use { bais =>
      new ByteArrayOutputStream(bytes.length) use { baos =>
        new GZIPInputStream(bais).use(IOUtils.copy(_, baos))
        baos.toByteArray
      }
    }
  }

  def compressSnappy(bytes: Array[Byte]): Array[Byte] = {
    new ByteArrayOutputStream(bytes.length) use { baos =>
      new SnappyOutputStream(baos).use(_.write(bytes))
      baos.toByteArray
    }
  }

  def decompressSnappy(bytes: Array[Byte]): Array[Byte] = {
    new ByteArrayOutputStream(bytes.length) use { baos =>
      new ByteArrayInputStream(bytes) use { bais =>
        new SnappyInputStream(bais).use(IOUtils.copy(_, baos))
      }
      baos.toByteArray
    }
  }

  def deserialize(bytes: Array[Byte]): AnyRef = {
    new ObjectInputStream(new ByteArrayInputStream(bytes)).use(_.readObject())
  }

  def serialize(value: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream(8192)
    val bytes = new ObjectOutputStream(baos) use { oos =>
      oos.writeObject(value)
      oos.flush()
      baos.toByteArray
    }
    bytes
  }

}
