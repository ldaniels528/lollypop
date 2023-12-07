package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.{ColumnType, Expression}
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.lollypop.runtime.{Scope, _}

/**
 * Represents a generic numeric data type
 */
abstract class NumericType extends FixedLengthNumericDataType(name = "Numeric", maxSizeInBytes = 128) with SerializedDataType[Number] {

  override def convert(value: Any): Number = value match {
    case Some(v) => convert(v)
    case b: Boolean => b.toInt
    case n: Number => n
    case s: String if s.contains("$") => BigDecimal(convertCurrency(name, s))
    case s: String => BigDecimal(s)
    case x => dieUnsupportedConversion(x, name)
  }

  override def getJDBCType: Int = java.sql.Types.NUMERIC

  override def isFloatingPoint: Boolean = true

  override def isNumeric: Boolean = true

  override def isSigned: Boolean = true

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Double] else classOf[Double]

}

object NumericType extends NumericType with ConstructorSupportCompanion with DataTypeParser {

  def apply(expression: Expression): NamedFunctionCall = {
    NamedFunctionCall(name = "Numeric", List(expression))
  }

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Number] | c.isDescendantOf(classOf[Number]) => Some(NumericType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Number => Some(NumericType)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case s if synonyms.contains(s) => Some(NumericType)
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("Numeric")

}
