package com.qwery.runtime.datatypes

import com.qwery.runtime.{DataObject, ResourceManager}

import java.io.{InputStream, OutputStream, RandomAccessFile}

/**
 * Base class for Large Objects (BLOB/CLOB/SQL-XML)
 */
trait AbstractLOB extends DataObject with AutoCloseable {
  ResourceManager.link(this)

  def raf: RandomAccessFile

  /**
   * This method frees the [[AbstractLOB]] object and releases the resources that
   * it holds. The object is invalid once the [[AbstractLOB.free]]
   * method is called.
   */
  def close(): Unit = {
    raf.close()
    ResourceManager.unlink(ns)
  }

  /**
   * This method frees the [[AbstractLOB]] object and releases the resources that
   * it holds. The object is invalid once the [[AbstractLOB.free]]
   * method is called.
   */
  def free(): Unit = close()

  /**
   * Returns the number of bytes in the [[AbstractLOB]] value
   * designated by this [[AbstractLOB]] object.
   * @return length of the [[AbstractLOB]] in bytes
   */
  def length(): Long = raf.length()

  def truncate(len: Long): Unit = raf.setLength(len)

  protected def createInputStream(): InputStream = new InputStream {
    private var offset: Long = -1

    override def read(): Int = {
      offset += 1
      raf.seek(offset)
      raf.read()
    }
  }

  protected def createInputStream(pos: Long, length: Long): InputStream = new InputStream {
    private var offset: Long = pos - 1

    override def read(): Int = {
      if (offset < pos + length) {
        offset += 1
        raf.seek(offset)
        raf.read()
      } else -1
    }
  }

  protected def createOutputStream(pos: Long): OutputStream = new OutputStream {
    private var offset: Long = pos - 1

    override def write(b: Int): Unit = {
      offset += 1
      raf.seek(offset)
      raf.write(b)
    }
  }

}