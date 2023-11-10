package com.lollypop.util

import com.lollypop.util.ResourceHelper.AutoClose
import org.apache.commons.io.IOUtils

import java.io._
import java.net.HttpURLConnection
import java.util.Base64

/**
 * I/O Tools
 */
object IOTools {

  def transfer(file: File, conn: HttpURLConnection): Int = {
    conn.getOutputStream.use(transfer(file, _))
  }

  def transfer(file: File, out: OutputStream): Int = {
    new FileInputStream(file).use(transfer(_, out))
  }

  def transfer(in: InputStream, conn: HttpURLConnection): Int = {
    conn.getOutputStream.use(transfer(in, _))
  }

  def transfer(in: InputStream, out: OutputStream): Int = IOUtils.copy(in, out)

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
