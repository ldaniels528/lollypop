package com.qwery.runtime.datatypes

import com.qwery.language.models.{ColumnType, Expression, Literal}
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream, dieUnsupportedConversion}
import com.qwery.runtime.{ONE_BYTE, Scope}
import com.qwery.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import java.util.Optional

/**
 * Represents a Boolean (Bit) type
 */
abstract class BooleanType extends FixedLengthDataType(maxSizeInBytes = ONE_BYTE, name = "Boolean")
  with FunctionalType[Boolean] {

  override def convert(value: Any): Boolean = value match {
    case null => false
    case b: Boolean => b
    case b: java.lang.Boolean => b
    case n: Number => n.doubleValue() != 0.0
    case o: Option[_] => o.nonEmpty
    case o: Optional[_] => !o.isEmpty
    case s: Seq[_] => s.nonEmpty
    case s: String => s.nonEmpty && java.lang.Boolean.valueOf(s)
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Boolean = buf.getBoolean

  override def encodeValue(value: Any): Array[Byte] = allocate(ONE_BYTE).putBoolean(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.BOOLEAN

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Boolean] else classOf[Boolean]

}

object BooleanType extends BooleanType with ConstructorSupportCompanion with DataTypeParser with ExpressionParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Boolean] => Some(BooleanType)
    case c if c == classOf[java.lang.Boolean] => Some(BooleanType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Boolean => Some(BooleanType)
    case _: java.lang.Boolean => Some(BooleanType)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(BooleanType) else None
  }

  override def synonyms: Set[String] = Set("Boolean")

  override def help: List[HelpDoc] = Nil

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts.nextIf("false")) Some(Literal(false))
    else if (ts.nextIf("true")) Some(Literal(true))
    else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = Seq("false", "true").exists(ts is _)

}