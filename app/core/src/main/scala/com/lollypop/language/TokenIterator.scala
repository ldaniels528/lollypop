package com.lollypop.language

import com.lollypop.language.Token._

import java.lang.String.copyValueOf

/**
 * Token Iterator
 * @author lawrence.daniels@gmail.com
 */
class TokenIterator(input: String) extends Iterator[Token] {
  private var pos = 0
  private val ca = input.toCharArray
  private val operators = "~!@#$%^&*()-_=+[]{}|\\;:,.<>?/".toCharArray
  private val compoundOperators = Seq(
    // bi-directional symbols
    "<>", "<|>",
    // uni-directional symbols
    "===>", "<===", "--->", "<---",
    "~>>", "<<~", "==>", "<==", "-->", "<--", "=>>",
    "~>", "<~", "->", "<-", "=>", "(%", "%)",
    // assignment & conditional operators and symbols
    "&&&=", "|||=", "///=", ":::=", ":::", ">>>=", "<<<=", "---=", "@@@=", "%%%=", "???=", "+++=", "***=", "^^^=",
    "&&=", "||=", "//=", "::=", "::", ">>=", "<<=", "--=", "@@=", "%%=", "??=", "++=", "**=", "^^=",
    "&=", "!=", "|=", "/=", ":=", ">=", "<=", "-=", "@=", "%=", "?=", "+=", "*=", "^=",
    "===", "==", ".?", ".!",
    // standard symbols
    "&&&", "|||", "...", "///", ">>>", "<<<", "---", "@@@", "%%%", "???", "+++", "***", "^^^",
    "&&", "||", "//", ">>", "<<", "--", "@@", "%%", "??", "++", "**", "^^",
    // file/http protocol symbols
    "://",
    "..", "./", ":/",
  )
  private val parsers = List(
    parseNumeric _, parseAlphaNumeric _,
    parseQuotesMultilineBQ _, parseQuotesMultilineDQ _, parseQuotesMultilineSQ _,
    parseQuotesBackticks _, parseQuotesDouble _, parseQuotesSingle _, parseProcessInvocation _,
    parseTable _, parseCompoundOperators _, parseOperators _, parseSymbols _)

  /**
   * Returns true, if at least one more non-whitespace character in the iterator
   * @return true, if at least one more non-whitespace character in the iterator
   */
  override def hasNext: Boolean = {
    var last: Int = 0
    do {
      last = pos
      if (pos == 0) skipComments(startCh = "#".toCharArray, endCh = "\n".toCharArray)
      skipComments(startCh = "/*".toCharArray, endCh = "*/".toCharArray)
      skipComments(startCh = "//".toCharArray, endCh = "\n".toCharArray)
      skipWhitespace()
    } while (last != pos)
    pos < ca.length
  }

  override def next(): Token = {
    if (!hasNext) throw new NoSuchElementException()
    else {
      val outcome = parsers.foldLeft[Option[Token]](None) { (token_?, parser) =>
        if (token_?.isEmpty) parser() else token_?
      }
      outcome.getOrElse(throw new IllegalArgumentException(copyValueOf(ca, pos, ca.length)))
    }
  }

  override def nextOption(): Option[Token] = {
    if (!hasNext) None
    else parsers.foldLeft[Option[Token]](None) { (token_?, parser) =>
      if (token_?.isEmpty) parser() else token_?
    }
  }

  def peek: Option[Token] = {
    val mark = pos
    val token_? = nextOption()
    pos = mark
    token_?
  }

  def span(length: Int): Option[String] = {
    if (pos + length <= ca.length) Some(copyValueOf(ca, pos, length)) else None
  }

  private def getLineNumber(position: Int): Int = 1 + input.take(position).count(_ == '\n')

  private def getColumnNumber(position: Int): Int = input.take(position).lastIndexOf('\n') match {
    case -1 => position
    case index => position - index
  }

  @inline
  private def hasMore: Boolean = pos < ca.length

  private def parseAlphaNumeric(): Option[AlphaNumericToken] = {
    val start = pos
    while (hasMore && (ca(pos).isLetterOrDigit || ca(pos) == '_' || ca(pos) >= 128)) pos += 1
    if (pos > start) Some(AlphaNumericToken(text = copyValueOf(ca, start, pos - start), lineNo = getLineNumber(start), columnNo = getColumnNumber(start))) else None
  }

  private def parseCompoundOperators(): Option[OperatorToken] = {
    compoundOperators.find(cop => hasMore && span(cop.length).contains(cop)) map { cop =>
      val start = pos
      pos += cop.length
      OperatorToken(text = cop, lineNo = getLineNumber(start), columnNo = getColumnNumber(start))
    }
  }

  private def parseNumeric(): Option[Token] = {
    def accept(ch: Char): Boolean = ca.length > pos + 1 && ca(pos) == ch && ca(pos + 1).isDigit

    val start = pos
    while (hasMore && (ca(pos).isDigit || accept('.'))) pos += 1
    if (pos > start) {
      val text = copyValueOf(ca, start, pos - start)
      if (text.count(_ == '.') > 1) Option(AlphaNumericToken(text, getLineNumber(start), getColumnNumber(start)))
      else Option(NumericToken(text = text, lineNo = getLineNumber(start), columnNo = getColumnNumber(start)))
    } else None
  }

  private def parseOperators(): Option[OperatorToken] = {
    if (hasMore && operators.contains(ca(pos))) {
      val start = pos
      pos += 1
      Option(OperatorToken(text = ca(start).toString, lineNo = getLineNumber(start), columnNo = getColumnNumber(start)))
    }
    else None
  }

  private def parseProcessInvocation(): Option[ProcessInvocationToken] = {
    parseSequence(enter = "(%", exit = "%)", f = ProcessInvocationToken.apply)
  }

  private def parseQuotes(ch: Char): Option[QuotedToken] = {
    if (hasMore && ca(pos) == ch) {
      val start = pos
      pos += 1
      while (hasMore && ca(pos) != ch) pos += 1
      pos += 1
      if (pos <= ca.length) {
        val length = pos - start
        val text = copyValueOf(ca, start, length)
        val value = copyValueOf(ca, start + 1, length - 2)
        Option(QuotedToken(text = text, value = value, lineNo = getLineNumber(start), columnNo = getColumnNumber(start), quoteChar = ch))
      } else {
        val length = ca.length - start
        val text = copyValueOf(ca, start, length)
        Option(QuotedToken(text = text, value = text, lineNo = getLineNumber(start), columnNo = getColumnNumber(start), quoteChar = ch))
      }
    }
    else None
  }

  private def parseQuotesBackticks(): Option[QuotedToken] = parseQuotes('`')

  private def parseQuotesDouble(): Option[QuotedToken] = parseQuotes('"')

  private def parseQuotesMultiline(q: Char, qLen: Int = 3): Option[TripleQuotedToken] = {
    val start = pos
    var p: Int = pos

    def hasMore: Boolean = p < ca.length

    def isMultilineQuote: Boolean = {
      val limit = p
      while (hasMore && (ca(p) == q) && (p - limit) < (qLen + (qLen - 1))) p += 1
      val isFound = (p - limit) >= qLen
      isFound
    }

    // if there's a multi-line quote (""")
    if (!isMultilineQuote) None else {
      do {
        // scan until we find a single quote (")
        while (hasMore && (ca(p) != q)) p += 1

        // continue to scan for a multi-line quote (""")
      } while (hasMore && !isMultilineQuote)
      pos = p // move the cursor to the end of the span
      val text = copyValueOf(ca, start, pos - start)
      val value = {
        val qq = String.valueOf(q) * qLen
        val a = if (text.startsWith(qq)) qLen else 0
        val b = if (text.endsWith(qq)) qLen else 0
        text.substring(a, text.length - b)
      }
      Option(TripleQuotedToken(text = text, value = value, lineNo = getLineNumber(start), columnNo = getColumnNumber(start), quoteChar = q))
    }
  }

  private def parseQuotesMultilineBQ(): Option[TripleQuotedToken] = parseQuotesMultiline(q = '`')

  private def parseQuotesMultilineDQ(): Option[TripleQuotedToken] = parseQuotesMultiline(q = '"')

  private def parseQuotesMultilineSQ(): Option[TripleQuotedToken] = parseQuotesMultiline(q = '\'')

  private def parseQuotesSingle(): Option[QuotedToken] = parseQuotes('\'')

  private def parseSymbols(): Option[SymbolToken] = {
    if (hasMore) {
      val start = pos
      pos += 1
      Some(SymbolToken(text = ca(start).toString, lineNo = getLineNumber(start), columnNo = getColumnNumber(start)))
    }
    else None
  }

  private def parseTable(): Option[TableToken] = {
    val start = pos
    var p = pos

    def hasMore: Boolean = p < ca.length

    if (hasMore && ca(p) == '|') {
      while (hasMore && ca(p) != '\n') p += 1
      val line = String.copyValueOf(ca, start, p - start).trim
      if (!line.startsWith("|") || !line.endsWith("|")) None
      else {
        pos = p
        val innards = line.drop(1).dropRight(1).trim
        val isComment = innards.startsWith("--")
        val values = if (isComment) Nil else innards.delimitedSplit('|').map(_.trim)
        Some(TableToken(text = line, value = values, lineNo = getLineNumber(start), columnNo = getColumnNumber(start)))
      }
    } else None
  }

  private def skipComments(startCh: Array[Char], endCh: Array[Char]): Unit = {

    def matches(chars: Array[Char]): Boolean = pos + (chars.length - 1) < ca.length &&
      chars.zipWithIndex.forall { case (ch, offset) => ch == ca(pos + offset) }

    if (matches(startCh)) {
      pos += startCh.length
      while (hasMore && !matches(endCh)) pos += 1
      pos += endCh.length
    }
  }

  private def parseSequence[A <: Token](enter: String, exit: String, f: (String, Int, Int) => A): Option[A] = {

    def isMatch(symbol: Array[Char], offset: Int): Boolean = {
      (offset + symbol.length < ca.length) &&
        (symbol zip ca.slice(offset, offset + symbol.length))
          .foldLeft(true) { case (agg, (a, b)) => agg && a == b }
    }

    val (_enter, _exit) = (enter.toCharArray, exit.toCharArray)
    if (isMatch(_enter, pos)) {
      val start: Int = pos
      while ((pos + _exit.length < ca.length) && !isMatch(_exit, pos)) pos += 1
      pos += _exit.length
      val limit = start + _enter.length
      val text = String.copyValueOf(ca, limit, pos - (limit + _exit.length))
      Some(f(text, getLineNumber(start), getColumnNumber(start)))
    } else None
  }

  private def skipWhitespace(): Unit = while (hasMore && ca(pos).isWhitespace) pos += 1

}

/**
 * Token Iterator Companion
 * @author lawrence.daniels@gmail.com
 */
object TokenIterator {
  def apply(input: String): TokenIterator = new TokenIterator(input)
}
