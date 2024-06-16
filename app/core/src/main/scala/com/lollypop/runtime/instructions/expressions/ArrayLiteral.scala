package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.AllFields.dieExpectedExpression
import com.lollypop.language.models.{Expression, Instruction, Literal}
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream, dieIllegalType}
import com.lollypop.runtime.datatypes.{ArrayType, DataType, Inferences}
import com.lollypop.runtime.plastics.Tuples.seqToArray
import com.lollypop.runtime.{LollypopVMSeqAddOns, Scope}
import lollypop.io.IOCost

/**
 * Represents an array literal
 * @param value the values within the array
 */
case class ArrayLiteral(value: List[Expression]) extends Literal
  with RuntimeExpression {
  private lazy val _type = ArrayType(Inferences.resolveType(value.map(Inferences.inferType)), capacity = Some(value.size))

  override def execute()(implicit scope: Scope): (Scope, IOCost, Array[_]) = {
    val (s, c, values) = value.evaluateAsArray(scope)
    (s, c, seqToArray(values))
  }

  override def returnType: DataType = _type

  override def toSQL: String = value.map(_.toSQL).mkString("[", ", ", "]")
}

/**
 * Array Expression Companion
 */
object ArrayLiteral extends ExpressionParser {

  def apply(items: Expression*): ArrayLiteral = ArrayLiteral(items.toList)

  def from(items: Any*): ArrayLiteral = ArrayLiteral(items.toList.map {
    case expr: Expression => expr
    case op: Instruction => dieIllegalType(op)
    case other => Literal(other)
  })

  override def help: List[HelpDoc] = {
    List(HelpDoc(
      name = "[]",
      category = CATEGORY_AGG_SORT_OPS,
      paradigm = PARADIGM_DECLARATIVE,
      syntax = "[ %E:items ]",
      description = "Separators are unnecessary between most statements",
      example = """x = ['A', 'B', 'C', 1, 5, 7] x ++ (x + 5)""",
      featureTitle = Some("No semicolons required")
    ))
  }

  /**
   * Parses an array expression (e.g. [1, 2, 3, 4] or ['A' to 'Z'])
   * @param stream the given [[TokenStream token stream]]
   * @return the option of a [[ArrayLiteral array expression]]
   */
  override def parseExpression(stream: TokenStream)(implicit compiler: SQLCompiler): Option[ArrayLiteral] = stream match {
    case ts if ts nextIf "[" =>
      var items: List[Expression] = Nil
      while (ts.isnt("]")) {
        // get an expression
        val expr0 = compiler.nextExpression(ts) || dieExpectedExpression()
        // if not the end of the array
        if (ts.isnt("]")) {
          // span/range: e.g. "1 to 5" and/or "1 until 5"?
          if (Span.understands(ts)) {
            val span = Span.parseInstructionChain(ts, expr0) || dieExpectedExpression()
            items = span :: items
          } else {
            items = expr0 :: items
          }
          // end (]) or continue (,)
          ts match {
            case ts if ts is "]" => ()
            case ts if ts nextIf "," => ()
            case ts => ts.die("Expected ] or ,")
          }
        } else {
          items = expr0 :: items
        }
      }
      ts.expect("]")
      Some(ArrayLiteral(items.reverse))
    case _ => None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "["

}