package com.lollypop.runtime.conversions

import com.lollypop.runtime._
import org.apache.commons.io.IOUtils

import java.io._
import java.net.HttpURLConnection

/**
 * Transfer Tools
 */
trait TransferTools {

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

}

/**
 * Transfer Tools Singleton
 */
object TransferTools extends TransferTools {

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

}
