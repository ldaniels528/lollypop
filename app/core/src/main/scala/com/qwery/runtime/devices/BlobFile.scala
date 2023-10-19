package com.qwery.runtime.devices

import com.qwery.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.qwery.runtime.{DatabaseObjectNS, DatabaseObjectRef, Scope}
import com.qwery.util.ResourceHelper._
import qwery.io.Encodable
import qwery.lang.Pointer

import java.io.{File, FileInputStream, RandomAccessFile}

/**
 * Simple BLOB file implementation
 * @param ns  the [[DatabaseObjectNS object namespace]]
 * @param raf the [[RandomAccessFile raw device]]
 */
class BlobFile(val ns: DatabaseObjectNS, val raf: RandomAccessFile) extends AutoCloseable {

  /**
   * Creates a new Blob File for the given namespace
   * @param ns the [[DatabaseObjectNS object namespace]]
   */
  def this(ns: DatabaseObjectNS) = this(ns, new RandomAccessFile(ns.blobDataFile, "rw"))

  def allocate(sizeInBytes: Int): Pointer = {
    val offset = raf.length()
    raf.setLength(offset + sizeInBytes)
    Pointer(offset, allocated = sizeInBytes, length = 0)
  }

  def append(bytes: Array[Byte]): Pointer = {
    val offset = raf.length()
    raf.seek(offset)
    raf.write(bytes)
    Pointer(offset, allocated = bytes.length, length = bytes.length)
  }

  def append(encodable: Encodable): Pointer = append(encodable.encode)

  def close(): Unit = {
    //ResourceManager.unlink(ns)
    raf.close()
  }

  def read(ptr: Pointer): Array[Byte] = {
    raf.seek(ptr.offset)
    val bytes = new Array[Byte](ptr.length.toInt)
    raf.read(bytes)
    bytes
  }

  def sizeInBytes: Long = raf.length()

  def update(ptr: Pointer, bytes: Array[Byte]): Pointer = {
    raf.seek(ptr.offset)
    raf.write(bytes)
    ptr.copy(length = bytes.length)
  }

  def update(ptr: Pointer, encodable: Encodable): Pointer = update(ptr, encodable.encode)

  def upload(file: File): Pointer = {
    val ptr = allocate(file.length().toInt)
    val bytes = new Array[Byte](1024)
    var (count, total) = (0, 0)
    raf.seek(ptr.offset)
    new FileInputStream(file).use { in =>
      do {
        count = in.read(bytes)
        if (count > 0) {
          raf.write(bytes, 0, count)
          total += count
        }
      } while (count != -1)
    }
    ptr.copy(length = total)
  }

}

object BlobFile {

  /**
   * Returns a Blob File for the given file
   * @param file the [[File]]
   * @return the [[BlobFile]]
   */
  def apply(ns: DatabaseObjectNS, file: File): BlobFile = new BlobFile(ns, new RandomAccessFile(file, "rw"))

  /**
   * Returns a Blob File for the given namespace
   * @param ns the [[DatabaseObjectNS object namespace]]
   * @return the [[BlobFile]]
   */
  def apply(ns: DatabaseObjectNS): BlobFile = BlobFile(ns, ns.blobDataFile)

  /**
   * Returns a Blob File for the given reference
   * @param ref the [[DatabaseObjectRef object reference]]
   * @return the [[BlobFile]]
   */
  def apply(ref: DatabaseObjectRef)(implicit scope: Scope): BlobFile = apply(ref.toNS)

}