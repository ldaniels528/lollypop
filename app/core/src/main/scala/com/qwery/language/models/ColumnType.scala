package com.qwery.language.models

import com.qwery.runtime.LONG_BYTES
import com.qwery.runtime.devices.FieldMetadata

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Represents a logical column type specification (e.g. "Decimal(12, 5)[10]")
 * @param name          the type name (.e.g. `Decimal`)
 * @param typeArgs      the optional size and precision (e.g. `Seq(12, 5)` or `Seq(ABC, DEF, GHI)`)
 * @param arrayArgs     the optional array dimensions (e.g. `Seq(10)`)
 * @param nestedColumns the optional nested data structure [[ParameterLike columns]]
 * @param isArray       true, if the type represents an array
 * @param isPointer     true, if the type is non-clustered (externally stored; a pointer)
 */
case class ColumnType(name: String,
                      typeArgs: Seq[String],
                      arrayArgs: Seq[String],
                      nestedColumns: Seq[ParameterLike],
                      isArray: Boolean = false,
                      isPointer: Boolean = false) extends Expression with SourceCodeInstruction {

  def isBlobTable: Boolean = isTable & isPointer

  def isClusteredTable: Boolean = isTable & !isBlobTable & !isMultiTenantTable

  def isMultiTenantTable: Boolean = isTable & !isArray & !isPointer

  def isTable: Boolean = name equalsIgnoreCase "Table"

  def size: Option[Int] = typeArgs.headOption.map(_.toInt)

  def precision: Option[Int] = typeArgs.lift(1).map(_.toInt)

  def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + size.getOrElse(LONG_BYTES)

  override def toSQL: String = {
    val sb = new mutable.StringBuilder(if (name.forall(c => c == '_' || c.isLetterOrDigit)) name else s"`$name`")
    if (typeArgs.nonEmpty) sb.append(s"(${typeArgs.map(qq).mkString(",")})")
    else if (nestedColumns.nonEmpty) sb.append(s"(${nestedColumns.map(_.toSQL).mkString(",")})")
    if (isArray) sb.append(s"[${arrayArgs.map(qq).mkString(",")}]")
    if (isPointer) sb.append("*")
    sb.toString()
  }

  private def qq(value: String): String = value match {
    case s if s.matches("\\d+") => s
    case s => '"' + s + '"'
  }

}

/**
 * ColumnType Companion
 */
object ColumnType {

  /**
   * Creates a new column type specification
   * @param name the type name (.e.g. "SMALLINT")
   * @return a new [[ColumnType column type specification]]
   */
  def apply(name: String): ColumnType = {
    new ColumnType(name, typeArgs = Nil, arrayArgs = Nil, nestedColumns = Nil)
  }

  /**
   * Creates a new column type specification
   * @param name the type name (.e.g. "String")
   * @param size the maximum size of the type (e.g. `32` <- "String(32)")
   * @return a new [[ColumnType column type specification]]
   */
  def apply(name: String, size: Int): ColumnType = {
    new ColumnType(name, typeArgs = Seq(size.toString), arrayArgs = Nil, nestedColumns = Nil)
  }

  /**
   * Creates a new column type specification
   * @param name      the type name (.e.g. "Decimal")
   * @param size      the maximum size of the type (e.g. `12` <- "Decimal(12, 5)")
   * @param precision the maximum precision of the type (e.g. `5` <- "Decimal(12, 5)")
   * @return a new [[ColumnType column type specification]]
   */
  def apply(name: String, size: Int, precision: Int): ColumnType = {
    new ColumnType(name, typeArgs = Seq(size.toString, precision.toString), arrayArgs = Nil, nestedColumns = Nil)
  }

  /**
   * Creates a new Array column type specification
   * @param columnType the item/element [[ColumnType type]]
   * @return a new [[ColumnType column type specification]]
   */
  def array(columnType: ColumnType): ColumnType = columnType.copy(isArray = true, arrayArgs = Nil)

  /**
   * Creates a new Array column type specification
   * @param columnType the item/element [[ColumnType type]]
   * @param arraySize  the maximum array capacity
   * @return a new [[ColumnType column type specification]]
   */
  def array(columnType: ColumnType, arraySize: Int): ColumnType = columnType.copy(isArray = true, arrayArgs = Seq(arraySize.toString))

  /**
   * Creates a new Class column type specification
   * @param className the class name
   * @return a new [[ColumnType column type specification]]
   */
  def `class`(className: String): ColumnType = {
    new ColumnType(name = "Class", typeArgs = Seq(className), arrayArgs = Nil, nestedColumns = Nil)
  }

  /**
   * Creates a new Enum column type specification
   * @param enumValues the collection of enumerated values
   * @return a new [[ColumnType column type specification]]
   */
  def enum(enumValues: Seq[String]): ColumnType = {
    new ColumnType(name = "Enum", typeArgs = enumValues, arrayArgs = Nil, nestedColumns = Nil)
  }

  /**
   * Creates a new Object (Any) column type specification
   * @param className the class name
   * @return a new [[ColumnType column type specification]]
   */
  def `object`(className: String): ColumnType = {
    new ColumnType(name = "Any", typeArgs = Seq(className), arrayArgs = Nil, nestedColumns = Nil)
  }

  /**
   * Creates a new Table column type specification
   * @param columns  the nested table [[Column columns]]
   * @param capacity the optional table capacity; expressed in terms of rows
   * @return a new [[ColumnType column type specification]]
   */
  def table(columns: Seq[Column], capacity: Int = 0, isPointer: Boolean = false): ColumnType = {
    new ColumnType(
      name = "Table",
      nestedColumns = columns,
      isPointer = isPointer,
      isArray = capacity > 0,
      arrayArgs = if (capacity == 0) Nil else Seq(capacity.toString),
      typeArgs = Nil)
  }

}