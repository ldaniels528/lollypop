package com.qwery.language

import com.qwery.language.Token.NumericToken
import com.qwery.language.models.ColumnType

/**
 * Represents a Column Type Parser
 */
trait ColumnTypeParser extends LanguageParser {

  /**
   * Parses the next data type (e.g. "Decimal(10, 2)") from the stream
   * @param stream the given [[TokenStream token stream]]
   * @return a [[ColumnType type]]
   */
  def parseColumnType(stream: TokenStream)(implicit compiler: SQLCompiler): Option[ColumnType]

}

/**
 * Column Type Parser
 */
object ColumnTypeParser extends ColumnTypeParser {
  override val help: List[HelpDoc] = Nil

  /**
   * Parses the next data type (e.g. "Decimal(10, 2)") from the stream
   * @param stream the given [[TokenStream token stream]]
   * @return a [[ColumnType type]]
   */
  def nextColumnType(stream: TokenStream)(implicit compiler: SQLCompiler): ColumnType = {
    var columnType: ColumnType = compiler.ctx.getColumnType(stream) getOrElse {
      // create the column type specification
      ColumnType(stream.next().valueAsString)
        // is there a size or precision? (e.g. "String(20)" or "DECIMAL(10,2)")
        .copy(typeArgs = stream.captureIf("(", ")", delimiter = Some(",")) { ts =>
          if (!ts.isNumeric) ts.dieExpectedNumeric() else ts.next().valueAsString
        })
    }

    // is this an array definition? (e.g. "Byte[]", "Int[5]" or "String(32)[10]")
    if (stream is "[") {
      stream.mark()
      val arrayTokens = stream.captureIf[Token]("[", "]", delimiter = Some(","))(_.next())
      if (arrayTokens.isEmpty | (arrayTokens.size == 1 & arrayTokens.forall(_.isInstanceOf[NumericToken]))) {
        columnType = columnType.copy(isArray = true, arrayArgs = arrayTokens.map(_.valueAsString))
        stream.commit()
      } else
        stream.rollback()
    }

    // is it a pointer type? (e.g. "String(80)*")
    columnType = if (stream.nextIf("*")) columnType.copy(isPointer = true) else columnType

    // return the column type
    columnType
  }

  override def parseColumnType(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ColumnType] = {
    if (understands(ts) || ts.isBackticks) Some(nextColumnType(ts)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    QweryUniverse().dataTypeParsers.flatMap(_.synonyms).exists(ts is _)
  }

}
