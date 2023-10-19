package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.language.models.ColumnType
import com.qwery.runtime.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.qwery.runtime.devices.FieldMetadata
import com.qwery.runtime.{INT_BYTES, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment

import scala.util.Try

/**
 * Represents any JVM Native type
 * @param className the JVM class name
 */
case class AnyType(className: String) extends AbstractDataType(name = "Any") with SerializedDataType[Any] {
  private lazy val _class = Class.forName(className)

  override def construct(args: Seq[Any]): Any = {
    _class.instantiate(args: _*)
  }

  override def convert(value: Any): Any = value

  override def getJDBCType: Int = java.sql.Types.JAVA_OBJECT

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + INT_BYTES + maxSizeInBytes

  override def maxSizeInBytes: Int = 16384

  override def toColumnType: ColumnType = ColumnType(name = name)

  override def toJavaType(hasNulls: Boolean): Class[_] = Class.forName(className)

  override def toSQL: String = s"`$className`"
}

/**
 * AnyType Companion
 */
object AnyType extends AnyType(className = "java.lang.Object") with ConstructorSupportCompanion with DataTypeParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = Some(AnyType(`class`.getName))

  override def getCompatibleValue(value: Any): Option[DataType] = Some(AnyType(value.getClass.getName))

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    // attempt to match a an existing type
    val overrideDataType_? : Option[DataType] = for {
      _class <- Try(Class.forName(columnType.name)).toOption
      _dataType <- QweryUniverse._dataTypeParsers.flatMap(_.getCompatibleType(_class)).headOption
    } yield _dataType

    overrideDataType_? ?? {
      columnType.name match {
        case s if synonyms.contains(s) =>
          columnType.typeArgs.toList match {
            case Nil => Some(AnyType)
            case className :: Nil => Some(AnyType(className))
            case other => dieNoSuchClass(other.mkString(", "))
          }
        case s if s.contains('.') => Some(AnyType(columnType.name))
        case _ => None
      }
    }
  }

  override def synonyms: Set[String] = Set("Any")

}
