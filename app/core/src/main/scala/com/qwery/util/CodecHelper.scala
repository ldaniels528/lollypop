package com.qwery.util

import com.qwery.util.ResourceHelper.AutoClose
import org.apache.commons.io.IOUtils
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

import java.io._
import java.util.Base64
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

  private def makeString(reader: Reader, delim: String): String = {
    val sb = new StringBuilder()
    val bufReader = new BufferedReader(reader)
    var line: String = null
    do {
      line = bufReader.readLine()
      if (line != null) sb.append(line).append(delim)
    } while (line != null)
    sb.toString().trim
  }

  class LimitedInputStream(host: InputStream, length: Long) extends InputStream {
    private var offset: Long = 0

    override def read(): Int = {
      if (offset < length) {
        offset += 1
        host.read()
      } else -1
    }
  }

  class LimitedReader(host: Reader, length: Long) extends Reader {
    private var offset: Long = 0

    override def read(chars: Array[Char], off: Int, len: Int): Int = {
      val adjustedLen = if (offset + len <= length) len else (length - offset).toInt
      if (adjustedLen > 0) {
        offset += len
        host.read(chars, off, adjustedLen)
      } else -1
    }

    override def close(): Unit = host.close()
  }

  final implicit class EnrichedByteArray(val bytes: Array[Byte]) extends AnyVal {

    def toBase64: String = Base64.getEncoder.encodeToString(bytes)

  }

  final implicit class EnrichedByteString(val string: String) extends AnyVal {

    def fromBase64: Array[Byte] = Base64.getDecoder.decode(string.getBytes())

  }

  final implicit class RichInputStream(val in: InputStream) extends AnyVal {

    def limitTo(length: Long) = new LimitedInputStream(in, length)

    def mkString(delim: String = "\n"): String = makeString(reader = new InputStreamReader(in), delim)

    def toBytes: Array[Byte] = {
      val out = new ByteArrayOutputStream()
      IOUtils.copyLarge(in, out)
      out.toByteArray
    }

    def toBase64: String = Base64.getEncoder.encodeToString(toBytes)

  }

  final implicit class RichReader(val reader: Reader) extends AnyVal {

    def limitTo(length: Long) = new LimitedReader(reader, length)

    def mkString(delim: String = "\n"): String = makeString(reader, delim)

    def toBase64: String = Base64.getEncoder.encodeToString(mkString().getBytes())

  }

}
