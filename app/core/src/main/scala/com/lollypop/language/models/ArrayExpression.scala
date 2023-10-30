package com.lollypop.language.models

import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.expressions.{ArrayFromRange, ArrayLiteral}

/**
 * Represents an array as an expression
 */
trait ArrayExpression extends Expression

/**
 * Array Expression Companion
 */
object ArrayExpression extends ExpressionParser {

  def inclusive(a: Expression, b: Expression): ArrayExpression = ArrayFromRange.Inclusive(a, b)

  def exclusive(a: Expression, b: Expression): ArrayExpression = ArrayFromRange.Exclusive(a, b)

  def fromValues(a: Expression*): ArrayExpression = ArrayLiteral(a.toList)

  override def help: List[HelpDoc] = Nil

  /**
   * Parses an array expression (e.g. [1, 2, 3, 4] or ['A' to 'Z'])
   * @param stream the given [[TokenStream token stream]]
   * @return the option of a [[ArrayExpression array expression]]
   */
  override def parseExpression(stream: TokenStream)(implicit compiler: SQLCompiler): Option[ArrayExpression] = stream match {
    case ts if ts nextIf "[" =>
      // is it an inclusive range (e.g. ['A' to 'Z'])?
      val array = if (ts.peekAhead(1).exists(_ is "to")) {
        for {
          expr0 <- compiler.nextExpression(ts)
          _ = ts.expect("to")
          expr1 <- compiler.nextExpression(ts)
        } yield inclusive(expr0, expr1)
      }

      // is it an exclusive range (e.g. ['A' until 'Z'])?
      else if (ts.peekAhead(1).exists(_ is "until")) {
        for {
          expr0 <- compiler.nextExpression(ts)
          _ = ts.expect("until")
          expr1 <- compiler.nextExpression(ts)
        } yield exclusive(expr0, expr1)
      }

      // must be an array literal (e.g. ['A', 'B', 'C', 'D'])
      else {
        var elems: List[Expression] = Nil
        while (ts isnt "]") {
          compiler.nextExpression(ts).foreach(expr => elems = expr :: elems)
          if (ts isnt "]") ts.expect(",")
        }
        Some(fromValues(elems.reverse: _*))
      }
      ts.expect("]")
      array
    case _ => None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "["

}