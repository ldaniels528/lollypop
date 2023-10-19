package com.qwery.language.models

import com.qwery.language.TemplateProcessor.TokenStreamExtensions
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models.Literal.implicits.NumericLiteralTokenStreamExtensions
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.QweryNative
import com.qwery.runtime.instructions.conditions.BooleanLiteral
import com.qwery.runtime.instructions.expressions.{AnyLiteral, ArrayLiteral}
import com.qwery.util.StringRenderHelper.StringRenderer
import qwery.lang.Null

/**
 * Represents a literal value (e.g. "Hello")
 */
trait Literal extends Expression with QweryNative {

  override def isPrimitive: Boolean = {
    value.asInstanceOf[AnyRef] match {
      case _: java.lang.Boolean => true
      case _: java.lang.Character => true
      case _: java.lang.Number => true
      case _ => false
    }
  }

  /**
   * @return the value
   */
  def value: Any

  override def toSQL: String = value.renderAsJson

}

object Literal extends ExpressionParser {

  /**
   * Creates a literal value (e.g. "Hello")
   * @param value the given value
   */
  def apply(value: Any): Literal = value match {
    case null => Null()
    case a: Literal => a
    case b: Boolean => BooleanLiteral(b)
    case a: Array[_] => ArrayLiteral(a.map(_.v).toList)
    case s: Seq[_] => ArrayLiteral(s.map(_.v).toList)
    case v => AnyLiteral(v)
  }

  def unapply(l: Literal): Option[Any] = Some(l.value)

  object implicits {

    /**
     * Numeric Literal TokenStream Extensions
     * @param ts the given [[TokenStream]]
     */
    final implicit class NumericLiteralTokenStreamExtensions(val ts: TokenStream) extends AnyVal {

      @inline def isConstant: Boolean = ts.isNumeric || ts.isQuoted

      @inline def isBinaryLiteral: Boolean = isNumericLiteral("b")

      @inline def isHexLiteral: Boolean = isNumericLiteral("x")

      @inline def isOctalLiteral: Boolean = isNumericLiteral("o")

      private def isNumericLiteral(escapeChar: String): Boolean = {
        (for {
          t0 <- ts.peek if t0 is "0"
          t1 <- ts.peekAhead(1) if (t1.text startsWith escapeChar) && (t1.columnNo == t0.columnNo + 1)
        } yield true).contains(true)
      }

    }

  }

  override def help: List[HelpDoc] = Nil

  override def parseExpression(stream: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    stream match {
      // is it a constant?
      case ts if ts.isBinaryLiteral => getBinaryLiteral(ts)
      case ts if ts.isHexLiteral => getHexLiteral(ts)
      case ts if ts.isOctalLiteral => getOctalLiteral(ts)
      case ts if ts.isCharacter => Option(Literal(value = ts.next().valueAsString.head))
      case ts if ts.isMultilineDoubleQuoted => Option(Literal(ts.next().valueAsString))
      case ts if ts.isMultilineSingleQuoted => Option(Literal(ts.next().valueAsString))
      case ts if ts.isDoubleQuoted & compiler.ctx.escapeCharacter == '"' => compiler.nextField(ts)
      case ts if ts.isConstant => parseConstant(ts)
      // is it a field or column type? (e.g. "Symbol" or "A.Symbol" or "Byte[]")?
      case ts if ts.isField => compiler.nextFieldOrColumnType(ts)
      // nothing matched
      case _ => None
    }
  }

  private def parseConstant(stream: TokenStream): Option[Literal] = {
    val tok0 = stream.next()
    val value = tok0.value
    value match {
      case number: Number if stream.peek.map(_.columnNo) contains (tok0.columnNo + 1) =>
        val modifiedNumber = stream match {
          case ts if ts.nextIf("b") => number.byteValue()
          case ts if ts.nextIf("d") => number.doubleValue()
          case ts if ts.nextIf("f") => number.floatValue()
          case ts if ts.nextIf("L") => number.longValue()
          case ts if ts.nextIf("s") => number.shortValue()
          case _ => value
        }
        Option(Literal(modifiedNumber))
      case _ => Option(Literal(value))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts.isConstant

  private def getBinaryLiteral(ts: TokenStream): Option[Literal] = {
    def isBinaryDigit(c: Char) = c == '0' || c == '1'

    def isBinaryLong(binString: String) = binString.length > 31

    if (ts.isBinaryLiteral) {
      ts.next()
      ts.peek match {
        case Some(t) =>
          val binString = t.text.drop(1)
          if (binString.forall(isBinaryDigit)) {
            ts.next()
            Option(Literal(
              if (isBinaryLong(binString)) java.lang.Long.parseLong(binString, 2)
              else Integer.parseInt(binString, 2)
            ))
          }
          else ts.die(s"Malformed binary literal '0${t.text}'")
      }
    } else None
  }

  private def getHexLiteral(ts: TokenStream): Option[Literal] = {
    def isHexDigit(c: Char) = c.isDigit || (c >= 'a' && c <= 'f')

    def isHexLong(hexString: String) = (hexString.length > 8) || (hexString.length >= 7 && hexString.head >= '8')

    if (ts.isHexLiteral) {
      ts.next()
      ts.peek match {
        case Some(t) =>
          val hexString = t.text.drop(1).toLowerCase()
          if (hexString.forall(isHexDigit)) {
            ts.next()
            Option(Literal(
              if (isHexLong(hexString)) java.lang.Long.parseLong(hexString, 16)
              else Integer.parseInt(hexString, 16)
            ))
          }
          else ts.die(s"Malformed hexadecimal literal '0${t.text}'")
      }
    } else None
  }

  private def getOctalLiteral(ts: TokenStream): Option[Literal] = {
    def isOctalDigit(c: Char) = c >= '0' && c <= '7'

    def isOctalLong(octalString: String) = (octalString.length > 11) || (octalString.length >= 10 && octalString.head >= '2')

    if (ts.isOctalLiteral) {
      ts.next()
      ts.peek match {
        case Some(t) =>
          val octalString = t.text.drop(1)
          if (octalString.forall(isOctalDigit)) {
            ts.next()
            Option(Literal(
              if (isOctalLong(octalString)) java.lang.Long.parseLong(octalString, 8)
              else Integer.parseInt(octalString, 8)
            ))
          }
          else ts.die(s"Malformed octal literal '0${t.text}'")
      }
    } else None
  }

}

