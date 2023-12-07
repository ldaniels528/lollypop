package com.lollypop.language

import com.lollypop.die
import com.lollypop.runtime.ModelStringRenderer.ModelStringRendering
import com.lollypop.runtime._

/**
 * Represents a token
 * @author lawrence.daniels@gmail.com
 */
sealed trait Token {

  /**
   * Indicates whether the given text matches the current token
   * @param value the given text value
   * @return true, if the value matches the current token
   */
  def is(value: String): Boolean = text == value

  /**
   * Indicates whether the given text does not match the current token
   * @param value the given text value
   * @return true, if the value matches the current token
   */
  def isnt(value: String): Boolean = !is(text)

  /**
   * @return the line number of this token
   */
  var lineNo: Int = -1

  /**
   * @return the column number of this token
   */
  var columnNo: Int = -1

  /**
   * Indicates whether the underlying text matches the given pattern
   * @param pattern the given pattern
   * @return true, if the underlying text matches the given pattern
   */
  def matches(pattern: String): Boolean = valueAsString matches pattern

  /**
   * @return the text contained by this token
   */
  def text: String

  override def toString: String = this.asModelString

  /**
   * @return the typed value contained by this token
   */
  def value: Any

  def valueAsString: String = String.valueOf(value)

}

/**
 * Token Companion
 */
object Token {

  def unapply(t: Token): Option[(String, Any, Int, Int)] = Some((t.text, t.value, t.lineNo, t.columnNo))

  /**
   * Represents an alphanumeric token
   * @author lawrence.daniels@gmail.com
   */
  case class AlphaNumericToken(text: String) extends AtomToken {
    override def value: String = text
  }

  object AlphaNumericToken {
    def apply(text: String, lineNo: Int, columnNo: Int): AlphaNumericToken = {
      AlphaNumericToken(text).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: AlphaNumericToken): Option[(String, Int, Int)] = Some((t.text, t.lineNo, t.columnNo))
  }

  /**
   * Represents an atom token (e.g. counter -or- `the counter`)
   * @author lawrence.daniels@gmail.com
   */
  sealed trait AtomToken extends Token

  object AtomToken {
    def unapply(t: AtomToken): Option[(String, Int, Int)] = Some((t.text, t.lineNo, t.columnNo))
  }

  /**
   * Represents a quoted token
   * @author lawrence.daniels@gmail.com
   */
  sealed trait QuotedToken extends Token {
    def isMultiline: Boolean = false

    def isBackTicks: Boolean = quoteChar == '`'

    def isDoubleQuoted: Boolean = quoteChar == '"'

    def isSingleQuoted: Boolean = quoteChar == '\''

    def quoteChar: Char

    override def value: String
  }

  object QuotedToken {
    def apply(text: String, value: String, lineNo: Int, columnNo: Int, quoteChar: Char, isMultiline: Boolean = false): QuotedToken = {
      if (isMultiline) TripleQuotedToken(text, value, lineNo, columnNo, quoteChar)
      else {
        quoteChar match {
          case '\'' => SingleQuotedToken(text, value, lineNo, columnNo)
          case '"' => DoubleQuotedToken(text, value, lineNo, columnNo)
          case '`' => BackticksQuotedToken(text, value, lineNo, columnNo)
          case c => die(s"Illegal quote character '$c'")
        }
      }
    }

    def unapply(t: QuotedToken): Option[(String, String, Int, Int, Char, Boolean)] = {
      Some((t.text, t.value, t.lineNo, t.columnNo, t.quoteChar, t.isMultiline))
    }
  }

  case class BackticksQuotedToken(text: String, value: String) extends AtomToken with QuotedToken {
    val quoteChar: Char = '`'
  }

  object BackticksQuotedToken {
    def apply(text: String, value: String, lineNo: Int, columnNo: Int): BackticksQuotedToken = {
      BackticksQuotedToken(text, value).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: BackticksQuotedToken): Option[(String, String, Int, Int)] = Some((t.text, t.value, t.lineNo, t.columnNo))
  }

  case class DoubleQuotedToken(text: String, value: String) extends QuotedToken {
    val quoteChar: Char = '"'
  }

  object DoubleQuotedToken {
    def apply(text: String, value: String, lineNo: Int, columnNo: Int): DoubleQuotedToken = {
      DoubleQuotedToken(text, value).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: DoubleQuotedToken): Option[(String, String, Int, Int)] = Some((t.text, t.value, t.lineNo, t.columnNo))
  }

  /**
   * Represents a process invocation token
   * @example {{{
   *  (% iostat 1 5 %)
   * }}}
   */
  case class ProcessInvocationToken(text: String) extends Token {
    override def value: String = text
  }

  object ProcessInvocationToken {
    def apply(text: String, lineNo: Int, columnNo: Int): ProcessInvocationToken = {
      ProcessInvocationToken(text).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: ProcessInvocationToken): Option[(String, Int, Int)] = Some((t.text, t.lineNo, t.columnNo))
  }

  case class SingleQuotedToken(text: String, value: String) extends QuotedToken {
    val quoteChar: Char = '\''
  }

  object SingleQuotedToken {
    def apply(text: String, value: String, lineNo: Int, columnNo: Int): SingleQuotedToken = {
      SingleQuotedToken(text, value).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: SingleQuotedToken): Option[(String, Int, Int)] = Some((t.text, t.lineNo, t.columnNo))
  }

  sealed trait TripleQuotedToken extends QuotedToken {
    override val isMultiline: Boolean = true
  }

  object TripleQuotedToken {
    def apply(text: String, value: String, lineNo: Int, columnNo: Int, quoteChar: Char): TripleQuotedToken = {
      quoteChar match {
        case '\'' => TripleSingleQuotedToken(text, value, lineNo, columnNo)
        case '"' => TripleDoubleQuotedToken(text, value, lineNo, columnNo)
        case '`' => TripleBackticksQuotedToken(text, value, lineNo, columnNo)
        case c => die(s"Illegal quote character '$c'")
      }
    }

    def unapply(t: TripleQuotedToken): Option[(String, String, Int, Int, Char, Boolean)] = {
      Some((t.text, t.value, t.lineNo, t.columnNo, t.quoteChar, t.isMultiline))
    }
  }

  case class TripleBackticksQuotedToken(text: String, value: String) extends TripleQuotedToken {
    val quoteChar: Char = '`'
  }

  object TripleBackticksQuotedToken {
    def apply(text: String, value: String, lineNo: Int, columnNo: Int): TripleBackticksQuotedToken = {
      TripleBackticksQuotedToken(text, value).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: TripleBackticksQuotedToken): Option[(String, String, Int, Int)] = Some((t.text, t.value, t.lineNo, t.columnNo))
  }

  case class TripleDoubleQuotedToken(text: String, value: String) extends TripleQuotedToken {
    val quoteChar: Char = '"'
  }

  object TripleDoubleQuotedToken {
    def apply(text: String, value: String, lineNo: Int, columnNo: Int): TripleDoubleQuotedToken = {
      TripleDoubleQuotedToken(text, value).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: TripleDoubleQuotedToken): Option[(String, String, Int, Int)] = Some((t.text, t.value, t.lineNo, t.columnNo))
  }

  case class TripleSingleQuotedToken(text: String, value: String) extends TripleQuotedToken {
    val quoteChar: Char = '\''
  }

  object TripleSingleQuotedToken {
    def apply(text: String, value: String, lineNo: Int, columnNo: Int): TripleSingleQuotedToken = {
      TripleSingleQuotedToken(text, value).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: TripleSingleQuotedToken): Option[(String, Int, Int)] = Some((t.text, t.lineNo, t.columnNo))
  }

  /**
   * Represents a numeric token
   * @author lawrence.daniels@gmail.com
   */
  case class NumericToken(text: String) extends Token {
    override def value: AnyVal = text match {
      case s if s.matches("^(\\+|-)?\\d+$") => s.toLong ~> {
        case n if n == n.toInt => n.toInt
        case n => n
      }
      case x => x.toDouble
    }
  }

  object NumericToken {
    def apply(text: String, lineNo: Int, columnNo: Int): NumericToken = {
      NumericToken(text).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: NumericToken): Option[(String, Int, Int)] = Some((t.text, t.lineNo, t.columnNo))
  }

  /**
   * Represents an operator token
   * @author lawrence.daniels@gmail.com
   */
  case class OperatorToken(text: String) extends Token {
    override def value: String = text
  }

  object OperatorToken {
    def apply(text: String, lineNo: Int, columnNo: Int): OperatorToken = {
      OperatorToken(text).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: OperatorToken): Option[(String, Int, Int)] = Some((t.text, t.lineNo, t.columnNo))
  }

  /**
   * Represents a symbolic token
   * @author lawrence.daniels@gmail.com
   */
  case class SymbolToken(text: String) extends Token {
    override def value: String = text
  }

  object SymbolToken {
    def apply(text: String, lineNo: Int, columnNo: Int): SymbolToken = {
      SymbolToken(text).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: SymbolToken): Option[(String, Int, Int)] = Some((t.text, t.lineNo, t.columnNo))
  }

  /**
   * Represents a Table literal token
   * @example {{{
   * |------------------------------|
   * | symbol | exchange | lastSale |
   * |------------------------------|
   * | XYZ    | AMEX     |    31.95 |
   * | AAXX   | NYSE     |    56.12 |
   * | JUNK   | AMEX     |    97.61 |
   * |------------------------------|
   * }}}
   * @author lawrence.daniels@gmail.com
   */
  case class TableToken(value: List[String], text: String) extends Token

  object TableToken {
    def apply(text: String, value: List[String], lineNo: Int, columnNo: Int): TableToken = {
      TableToken(value, text).withLineAndColumn(lineNo, columnNo)
    }

    def unapply(t: TableToken): Option[(String, List[String], Int, Int)] = Some((t.text, t.value, t.lineNo, t.columnNo))
  }

}