package com.lollypop.runtime.datatypes

import com.lollypop.language._
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime._
import com.lollypop.runtime.devices.FieldMetadata
import lollypop.io.Decoder

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

/**
 * Represents a Enumeration type
 * @param values the supported enumerations
 */
case class EnumType(values: Seq[String]) extends FixedLengthDataType(name = "Enum", maxSizeInBytes = SHORT_BYTES)
  with Decoder[String] {

  override def convert(value: Any): Short = {
    value match {
      case Some(v) => convert(v)
      case n: Number =>
        val index = n.shortValue()
        assert(index >= 0 && index < values.length, s"Index is out of range ($index)")
        index
      case s: String =>
        values.indexOf(s) match {
          case -1 => die(s"'$s' is not allowed for this enumeration")
          case index => index.toShort
        }
      case x => dieUnsupportedConversion(x, name)
    }
  }

  override def decode(buf: ByteBuffer): String = values(buf.getShort)

  override def encodeValue(value: Any): Array[Byte] = allocate(SHORT_BYTES).putShort(convert(value)).flipMe().array()

  override def isEnum: Boolean = true

  override def getJDBCType: Int = java.sql.Types.SMALLINT

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + SHORT_BYTES

  override def toColumnType: ColumnType = ColumnType.enum(values)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[String]

  override def toSQL: String = s"$name${values.mkString("(", ", ", ")")}"
}

object EnumType extends EnumType(values = Nil) with ColumnTypeParser with DataTypeParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = None

  override def getCompatibleValue(value: Any): Option[DataType] = None

  override def help: List[HelpDoc] = Nil

  override def parseColumnType(stream: TokenStream)(implicit compiler: SQLCompiler): Option[ColumnType] = {
    if (understands(stream)) {
      val typeName = stream.next().valueAsString
      Option(ColumnType(typeName).copy(typeArgs =
        stream.captureIf("(", ")", delimiter = Some(",")) { ts =>
          if (!ts.isQuoted && !ts.isText && !ts.isBackticks) ts.dieExpectedEnumIdentifier() else ts.next().valueAsString
        }))
    } else None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(EnumType(values = columnType.typeArgs)) else None
  }

  override def synonyms: Set[String] = Set("Enum")

  override def toSQL: String = s"$name(${values.mkString("(", ", ", ")")})"

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = synonyms.exists(ts is _)

}