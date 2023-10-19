package com.qwery.runtime.devices

import com.qwery.language.dieNoSuchColumn
import com.qwery.language.models.{Column, Expression, Instruction, ParameterLike}
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.DataType

/**
 * Represents a table column
 * @param name         the name of the column
 * @param `type`       the [[DataType column type]]
 * @param defaultValue the optional default value
 */
case class TableColumn(name: String, `type`: DataType, defaultValue: Option[Expression] = None)
  extends Instruction {

  def getTypedDefaultValue: Option[Any] = defaultValue.map(`type`.convert)

  /**
   * @return true if the column is not marked as external
   * @see [[isExternal]]
   */
  def isClustered: Boolean = !isExternal

  /**
   * @return true if the column is marked as external (usually an Array, BLOB, CLOB, Table or Serializable)
   */
  def isExternal: Boolean = `type`.isExternal

  override def toSQL: String = (s"$name: ${`type`.toSQL}" :: defaultValue.toList.map(v => s"= ${v.toSQL}")).mkString(" ")

  override def toString: String = {
    f"""|${getClass.getSimpleName}(
        |name=$name,
        |type=${`type`},
        |defaultValue=${defaultValue.orNull},
        |)""".stripMargin.split("\n").mkString
  }

}

/**
 * Table Column Companion
 */
object TableColumn {

  /**
   * Implicit classes and conversions
   */
  object implicits {

    /**
     * SQL ParameterLike-To-TableColumn Conversion
     * @param parameterLike the [[ParameterLike SQL Column]]
     */
    final implicit class SQLToColumnConversion(val parameterLike: ParameterLike) extends AnyVal {

      @inline
      def toTableColumn(implicit scope: Scope): TableColumn = {
        TableColumn(name = parameterLike.name, `type` = DataType.load(parameterLike.`type`), defaultValue = parameterLike.defaultValue)
      }
    }

    /**
     * TableColumn-To-Column Conversion
     * @param column the [[TableColumn column]]
     */
    final implicit class TableColumnToSQLColumnConversion(val column: TableColumn) extends AnyVal {
      @inline
      def toColumn: Column = {
        Column(name = column.name, `type` = column.`type`.toColumnType, defaultValue = column.defaultValue)
      }
    }

    /**
     * TableColumn Sequences
     * @param columns the collection of [[TableColumn columns]]
     */
    final implicit class TableColumnSeq(val columns: Seq[TableColumn]) extends AnyVal {

      @inline
      def getIndexByName(name: String): Int = columns.indexWhere(_.name == name) match {
        case -1 => dieNoSuchColumn(name)
        case index => index
      }
    }
  }

}