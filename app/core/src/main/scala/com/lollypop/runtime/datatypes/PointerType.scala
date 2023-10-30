package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.{ColumnType, Expression}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall
import lollypop.io.Decoder
import lollypop.lang.Pointer

import java.nio.ByteBuffer

/**
 * Represents the Pointer Type
 */
class PointerType extends FixedLengthDataType(maxSizeInBytes = Pointer.SIZE_IN_BYTES, name = "Pointer")
  with Decoder[Pointer] with FunctionalType[Pointer] {

  override def construct(args: Seq[Any]): Pointer = {
    args.map(_.asInstanceOf[AnyRef]).toList match {
      case List(x: Number, y: Number, z: Number) => Pointer(x.longValue(), y.longValue(), z.longValue())
      case l => dieArgumentMismatch(args = l.size, minArgs = 3, maxArgs = 3)
    }
  }

  override def convert(value: Any): Pointer = value match {
    case Some(v) => convert(v)
    case s: String => Pointer.parse(s)
    case v: Pointer => v
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Pointer = Pointer.decode(buf)

  override def encodeValue(value: Any): Array[Byte] = convert(value).encode

  override def getJDBCType: Int = java.sql.Types.JAVA_OBJECT

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[Pointer]

  override def toSQL: String = name

}

/**
 * Pointer Type Singleton
 */
object PointerType extends PointerType with ConstructorSupportCompanion with DataTypeParser {

  def apply(offset: Expression, allocated: Expression, length: Expression): NamedFunctionCall = {
    NamedFunctionCall(name = "Pointer", List(offset, allocated, length))
  }

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Pointer] => Some(PointerType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Pointer => Some(PointerType)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case s if s == name =>
        columnType.typeArgs match {
          case Nil => Some(PointerType)
          case args => dieArgumentMismatch(args = args.length, minArgs = 0, maxArgs = 0)
        }
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("Pointer")

}
