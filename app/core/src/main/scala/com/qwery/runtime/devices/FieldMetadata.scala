package com.qwery.runtime.devices

import com.qwery.runtime.devices.FieldMetadata._

/**
  * Represents the metadata of a field stored in the database.
  * <pre>
  * ----------------------------------------
  * a - active bit ...... [1000.0000 ~ 0x80]
  * c - compressed bit .. [0100.0000 ~ 0x40]
  * ----------------------------------------
  * </pre>
  * @param isActive     indicates whether the field's data is active; meaning not null.
  * @param isCompressed indicates whether the field's data is compressed
  */
case class FieldMetadata(isActive: Boolean, isCompressed: Boolean) {

  /**
    * Encodes the [[FieldMetadata metadata]] into a bit sequence representing the metadata
    * @return a byte representing the metadata
    */
  def encode: Byte = {
    val a = if (isActive) ACTIVE_BIT else 0
    val c = if (isCompressed) COMPRESSED_BIT else 0
    (a | c).toByte
  }

  def isNotNull: Boolean = isActive

  def isNull: Boolean = !isActive

  override def toString: String =
    s"""|${getClass.getSimpleName}(
        |isActive=$isActive,
        |isCompressed=$isCompressed
        |)""".stripMargin.split("\n").mkString

}

/**
  * Field MetaData Companion
  */
object FieldMetadata {
  // bit enumerations
  val ACTIVE_BIT = 0x80
  val COMPRESSED_BIT = 0x40

  // the length of the encoded metadata
  val BYTES_LENGTH = 1

  /**
    * Creates new field metadata based on existing column metadata
    * @param column the [[TableColumn column]]
    * @return a new [[FieldMetadata field metadata]]
    */
  def apply(column: TableColumn): FieldMetadata = apply()

  /**
    * Creates new field metadata
    * @param isActive     indicates whether the field's data is active; meaning not null.
    * @param isCompressed indicates whether the field's data is compressed
    * @return a new [[FieldMetadata field metadata]]
    */
  def apply(isActive: Boolean = true, isCompressed: Boolean = false) = new FieldMetadata(
    isActive = isActive,
    isCompressed = isCompressed
  )

  /**
    * Decodes the 8-bit metadata code into [[FieldMetadata metadata]]
    * @param metadataBits the metadata byte
    * @return a new [[FieldMetadata metadata]]
    */
  def decode(metadataBits: Byte): FieldMetadata = apply(
    isActive = (metadataBits & ACTIVE_BIT) > 0,
    isCompressed = (metadataBits & COMPRESSED_BIT) > 0
  )

}