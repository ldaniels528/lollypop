package com.qwery.runtime.datatypes

import com.qwery.language.models.ColumnType
import com.qwery.language.{HelpDoc, LanguageParser, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope

/**
 * Represents a data type parser
 */
trait DataTypeParser extends LanguageParser {

  def getCompatibleType(`class`: Class[_]): Option[DataType]

  def getCompatibleValue(value: Any): Option[DataType]

  override def help: List[HelpDoc] = Nil

  def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType]

  def synonyms: Set[String]

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = synonyms.exists(ts is _)

}
