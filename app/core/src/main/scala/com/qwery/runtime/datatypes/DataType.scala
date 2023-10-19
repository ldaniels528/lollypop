package com.qwery.runtime.datatypes

import com.qwery.die
import com.qwery.language.models.{ColumnType, Instruction}
import com.qwery.language.{SQLCompiler, SQLTemplateParams, TokenStream, dieUnsupportedType}
import com.qwery.runtime.devices.FieldMetadata
import com.qwery.runtime.{DatabaseManagementSystem, DatabaseObjectNS, DatabaseObjectRef, Scope}
import com.qwery.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.Encoder

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec

/**
 * Represents a fully-realized data type
 */
trait DataType extends Instruction with Encoder {

  /**
   * @return the name of the data type
   */
  def name: String

  /**
   * Converts the given value into a value compatible with this type
   * @param value the given value
   * @return a value compatible with this type
   */
  def convert(value: Any): Any

  /**
   * Decodes the buffer as a value
   * @param buf the [[ByteBuffer buffer]]
   * @return the option of a value
   */
  def decode(buf: ByteBuffer): Any

  /**
   * Decodes the buffer as the option of a value
   * @param buf the [[ByteBuffer buffer]]
   * @return a tuple containing the [[FieldMetadata metadata]] and an option of a value
   */
  def decodeFull(buf: ByteBuffer): (FieldMetadata, Option[Any]) = {
    val fmd = buf.getFieldMetadata
    val data_? = if (fmd.isNotNull) Option(decode(buf)) else None
    (fmd, data_?)
  }

  /**
   * Encodes the given value into a byte buffer
   * @param value the option of a value
   * @return a new [[ByteBuffer buffer]]
   */
  @tailrec
  final def encode(value: Any): Array[Byte] = value match {
    case Some(v) => encode(v)
    case null | None => Array.empty[Byte]
    case x => encodeValue(x)
  }

  /**
   * Encodes the given value and [[FieldMetadata metadata]] into a byte buffer
   * @param value the option of a value
   * @return a new [[ByteBuffer buffer]]
   */
  final def encodeFull(value: Any): ByteBuffer = {
    val bytes = encode(value)
    val fmd = FieldMetadata(isActive = bytes.nonEmpty)
    allocate(FieldMetadata.BYTES_LENGTH + bytes.length).putFieldMetadata(fmd).put(bytes).flipMe()
  }

  /**
   * Encodes the given value into a byte buffer
   * @param value the option of a value
   * @return a new [[ByteBuffer buffer]]
   */
  def encodeValue(value: Any): Array[Byte]

  /**
   * @return the JDBC equivalent type
   */
  def getJDBCType: Int

  /**
   * @return true, if this type is auto-increment
   */
  def isAutoIncrement: Boolean = false

  /**
   * @return true, if this type is an enumeration
   */
  def isEnum: Boolean = false

  /**
   * @return true, if this type is externally stored
   */
  def isExternal: Boolean = false

  /**
   * @return true, if this type is externally stored
   */
  def isFixedLength: Boolean = false

  /**
   * @return true, if this type is a floating point number
   */
  def isFloatingPoint: Boolean = false

  /**
   * @return true, if this type is numeric
   */
  def isNumeric: Boolean = false

  /**
   * @return true, if this type is mathematically signed
   */
  def isSigned: Boolean = false

  /**
   * @return true, if the type refers to a table
   */
  def isTable: Boolean = false

  /**
   * Returns the maximum physical size of the column
   */
  def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + maxSizeInBytes

  /**
   * @return the maximum data length that can be expressed by instances of this type
   */
  def maxSizeInBytes: Int

  /**
   * @return the type's precision
   */
  def precision: Int = maxSizeInBytes

  /**
   * @return the type's scale
   */
  def scale: Int = maxSizeInBytes

  /**
   * @return the equivalent [[ColumnType column type]]
   */
  def toColumnType: ColumnType

  /**
   * @return the corresponding JVM class
   */
  def toJavaType(hasNulls: Boolean): Class[_]

  /**
   * @return the SQL representation of this type
   */
  override def toSQL: String = name

}

/**
 * Data Type Companion
 */
object DataType {

  /**
   * Creates a new data type instance
   * @param columnType the [[ColumnType column type]]
   * @param scope      the implicit [[Scope scope]]
   * @return a new [[DataType data type]] instance
   */
  def apply(columnType: ColumnType)(implicit scope: Scope): DataType = {
    compile(columnType) || dieUnsupportedType(columnType)
  }

  /**
   * Translates the column type into a data type
   * @param columnType the [[ColumnType column type]]
   * @param scope      the implicit [[Scope scope]]
   * @return the option of a [[DataType data type]] instance
   */
  def compile(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    // attempt to determine the data type
    val dataType_? = scope.getUniverse.dataTypeParsers
      .foldLeft[Option[DataType]](None) { (dataType, parser) => dataType ?? parser.parseDataType(columnType) }

    // is the data type an array?
    dataType_? map { dataType =>
      if (columnType.isArray && !columnType.isTable)
        ArrayType(dataType, capacity = if (columnType.arrayArgs.nonEmpty) Some(columnType.arrayArgs.map(_.toInt).product) else None)
      else
        dataType
    }
  }

  /**
   * Creates a new data type instance
   * @param columnType the [[ColumnType column type]]
   * @param scope      the implicit [[Scope scope]]
   * @return a new [[DataType data type]] instance
   */
  def load(columnType: ColumnType)(implicit scope: Scope): DataType = {
    compile(columnType) match {
      case Some(dataType) => dataType
      case None =>
        val dataType = load(DatabaseObjectRef(columnType.name).toNS)
        // is the data type an array?
        if (columnType.isArray && !columnType.name.equalsIgnoreCase("Table"))
          ArrayType(dataType, capacity = if (columnType.arrayArgs.nonEmpty) Some(columnType.arrayArgs.map(_.toInt).product) else None)
        else
          dataType
    }
  }

  /**
   * Loads a durable data type instance from disk
   * @param ns    the [[DatabaseObjectNS column type namespace]]
   * @param scope the implicit [[Scope scope]]
   * @return the loaded [[DataType data type]] instance
   */
  def load(ns: DatabaseObjectNS)(implicit scope: Scope): DataType = {
    DatabaseManagementSystem.readUserType(ns)
  }

  /**
   * Creates a new data type instance
   * @param spec  the column type specification (e.g. "Numeric(12,5)[10]")
   * @param scope the implicit [[Scope scope]]
   * @return a new [[DataType data type]] instance
   */
  def parse(spec: String)(implicit scope: Scope): DataType = {
    implicit val compiler: SQLCompiler = scope.getCompiler
    spec match {
      case "null" => AnyType
      case s =>
        val stp = SQLTemplateParams(TokenStream(s), "%T:type")
        apply(columnType = stp.types.getOrElse("type", die(s"Type '$s' was not resolved")))
    }
  }

}
