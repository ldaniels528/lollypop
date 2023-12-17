package com.lollypop.runtime.datatypes

import com.lollypop.language._
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime._
import com.lollypop.runtime.devices.FieldMetadata
import spray.json.JsValue

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

case class JsonType(maxSizeInBytes: Int, override val isExternal: Boolean = false)
  extends AbstractDataType(name = "JSON") with FunctionalType[JsValue] {

  override def convert(value: Any): JsValue = value match {
    case Some(v) => convert(v)
    case j: JsValue => j
    case x => x.toJsValue
  }

  override def decode(buf: ByteBuffer): JsValue = buf.getText.toJsValue

  override def encodeValue(value: Any): Array[Byte] = {
    convert(value).compactPrint.getBytes ~> { b => allocate(INT_BYTES + b.length).putInt(b.length).put(b).flipMe().array() }
  }

  override def getJDBCType: Int = java.sql.Types.VARCHAR

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + INT_BYTES + maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType(name = name, size = maxSizeInBytes)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[String]

  override def toSQL: String = s"$name($maxSizeInBytes)${if (isExternal) "*" else ""}"
}

/**
 * Represents a String type with a default size (256-bytes)
 */
object JsonType extends JsonType(maxSizeInBytes = 256, isExternal = true) with ConstructorSupportCompanion with DataTypeParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[JsValue] => Some(JsonType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case j: JsValue => Some(JsonType(j.compactPrint.length))
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case s if synonyms.contains(s) => Some((columnType.size.map(JsonType(_)) || JsonType).copy(isExternal = columnType.isPointer))
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("JSON")

}
