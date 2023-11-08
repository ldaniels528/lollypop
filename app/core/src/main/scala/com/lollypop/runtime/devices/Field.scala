package com.lollypop.runtime.devices

import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.{LollypopVM, Scope}

/**
 * Represents a field
 * @param name     the name of the field
 * @param metadata the [[FieldMetadata field metadata]] containing the field type and other useful information
 * @param value    the optional value of the field
 */
case class Field(name: String, metadata: FieldMetadata, var value: Option[Any])

/**
 * Field Companion
 */
object Field {

  final implicit class ColumnToFieldExtension(val column: TableColumn) extends AnyVal {

    @inline
    def toField(fmd: FieldMetadata = FieldMetadata())(implicit scope: Scope): Field = {
      val defaultValue = column.defaultValue.flatMap(expr => Option(expr.execute(scope)._3))
      Field(column.name, fmd, value = defaultValue)
    }

    @inline
    def withValue(value: Option[Any], fmd: FieldMetadata = FieldMetadata()): Field = Field(column.name, fmd, value)

  }

}