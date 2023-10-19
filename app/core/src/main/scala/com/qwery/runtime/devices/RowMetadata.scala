package com.qwery.runtime.devices

import com.qwery.runtime.devices.RowMetadata._
import com.qwery.util.StringRenderHelper.toProductString

/**
 * Represents the metadata of a row stored in the database.
 * <pre>
 * ---------------------------------------
 * a - allocated bit .. [1000.0000 ~ 0x80]
 * b - BLOB bit ....... [0100.0000 ~ 0x40]
 * e - encrypted bit .. [0010.0000 ~ 0x20]
 * r - replicated bit . [0001.0000 ~ 0x10]
 * ---------------------------------------
 * </pre>
 * @param isAllocated  indicates whether the row is allocated; meaning not deleted.
 * @param isBlob       indicates whether the row is part of a BLOB.
 * @param isEncrypted  indicates whether the row is encrypted
 * @param isReplicated indicates whether the row has been replicated
 */
case class RowMetadata(isAllocated: Boolean = true,
                       isBlob: Boolean = false,
                       isEncrypted: Boolean = false,
                       isReplicated: Boolean = false) {

  /**
   * Encodes the [[RowMetadata metadata]] into a bit sequence representing the metadata
   * @return a short representing the metadata bits
   */
  def encode: Byte = {
    val a = if (isAllocated) ALLOCATED_BIT else 0
    val b = if (isBlob) BLOB_BIT else 0
    val e = if (isEncrypted) ENCRYPTED_BIT else 0
    val r = if (isReplicated) REPLICATED_BIT else 0
    (b | a | r | e).toByte
  }

  def isActive: Boolean = isAllocated & !isBlob

  def isDeleted: Boolean = !isAllocated & !isBlob

  override def toString: String = toProductString(this)

}

/**
 * Row MetaData Companion
 */
object RowMetadata {
  // bit enumerations
  private val ALLOCATED_BIT = 0x80
  private val BLOB_BIT = 0x40
  private val ENCRYPTED_BIT = 0x20
  private val REPLICATED_BIT = 0x10

  // the length of the encoded metadata
  val BYTES_LENGTH = 1

  /**
   * Decodes the 8-bit metadata code into [[RowMetadata metadata]]
   * @param metadataBits the metadata byte
   * @return a new [[RowMetadata metadata]]
   */
  def decode(metadataBits: Byte): RowMetadata = new RowMetadata(
    isAllocated = (metadataBits & ALLOCATED_BIT) > 0,
    isBlob = (metadataBits & BLOB_BIT) > 0,
    isEncrypted = (metadataBits & ENCRYPTED_BIT) > 0,
    isReplicated = (metadataBits & REPLICATED_BIT) > 0
  )

}