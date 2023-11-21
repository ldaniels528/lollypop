package com.lollypop.runtime.datatypes

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.models.{ColumnType, Instruction}
import com.lollypop.runtime.devices.{FieldMetadata, QMap, RowCollection}
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.lollypop.runtime.{INT_BYTES, Scope}
import com.lollypop.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}
import com.lollypop.util.DateHelper
import com.lollypop.util.JVMSupport.NormalizeAny
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.runtime.conversions.TransferTools.RichInputStream
import com.lollypop.util.StringRenderHelper.StringRenderer
import spray.json.JsValue

import java.io.File
import java.nio.ByteBuffer.allocate
import java.nio.{ByteBuffer, CharBuffer}
import java.util.Date
import scala.collection.mutable

/**
 * Represents a String type
 * @param maxSizeInBytes the maximum character length of the string
 */
case class StringType(maxSizeInBytes: Int, override val isExternal: Boolean = false)
  extends AbstractDataType(name = "String") with FunctionalType[String] {

  override def convert(value: Any): String = value.normalize match {
    case None | null => null
    case Some(v) => convert(v)
    case a: Array[Byte] => new String(a)
    case a: Array[Char] => String.copyValueOf(a)
    case a: Array[Character] => String.copyValueOf(a.filterNot(_ == null).map(_.charValue()))
    case a: Array[_] => a.renderAsJson
    case b: RowCollection => b.renderAsJson
    case b: java.sql.Blob => b.getBinaryStream.mkString()
    case b: ByteBuffer => new String(b.array())
    case c: CharBuffer => String.valueOf(c.array())
    case c: Class[_] => c.getName
    case c: java.sql.Clob => c.getAsciiStream.mkString()
    case d: Date => DateHelper.format(d)
    case f: File => f.getCanonicalPath
    case i: Instruction => i.toSQL
    case j: JsValue => j.compactPrint
    case m: QMap[_, _] => m.renderAsJson
    case n: Number => String.valueOf(n)
    case b: java.sql.SQLXML => b.getBinaryStream.mkString()
    case s: String => s
    case x => x.render
  }

  override def decode(buf: ByteBuffer): String = buf.getText

  override def encodeValue(value: Any): Array[Byte] = {
    convert(value) match {
      case null => new Array(0)
      case s => s.getBytes ~> { b => allocate(INT_BYTES + b.length).putInt(b.length).put(b).flipMe().array() }
    }
  }

  override def getJDBCType: Int = java.sql.Types.VARCHAR

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + INT_BYTES + maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType(name = name, size = maxSizeInBytes)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[String]

  override def toSQL: String = s"$name($maxSizeInBytes)"
}

/**
 * Represents a String type with a default size (256-bytes)
 */
object StringType extends StringType(maxSizeInBytes = 256, isExternal = true) with ConstructorSupportCompanion with DataTypeParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[String] => Some(StringType)
    case c if c == classOf[mutable.StringBuilder] => Some(StringType)
    case c if c == classOf[StringBuffer] => Some(StringType)
    case c if c.isDescendantOf(classOf[CharSequence]) => Some(StringType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case s: CharSequence => Some(StringType(s.length))
    case s: mutable.StringBuilder => Some(StringType(s.length))
    case s: StringBuffer => Some(StringType(s.length))
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms.contains(columnType.name))
      Some((columnType.size.map(StringType(_)) || StringType).copy(isExternal = columnType.isPointer))
    else
      None
  }

  override def synonyms: Set[String] = Set("String")

}
