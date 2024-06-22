package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.AllFields.dieExpectedExpression
import com.lollypop.language.models.Expression
import com.lollypop.language.{HelpDoc, InstructionPostfixParser, SQLCompiler, TokenStream, dieUnsupportedType}
import com.lollypop.runtime.datatypes.Inferences.fromValue
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.{LollypopVMAddOns, Scope}
import lollypop.io.IOCost

/**
 * Represents a span or range of values
 */
sealed trait Span extends RuntimeExpression {

  def start: Expression

  def end: Expression

  override def toSQL: String

}

/**
 * Span Companion
 */
object Span extends InstructionPostfixParser[Expression] {

  override def help: List[HelpDoc] = {
    List(HelpDoc(
      name = "to",
      category = CATEGORY_AGG_SORT_OPS,
      paradigm = PARADIGM_DECLARATIVE,
      syntax = "%e:a to %e:b",
      description = "Defines an inclusive range of values",
      example = "'A' to 'G'",
      featureTitle = None
    ), HelpDoc(
      name = "until",
      category = CATEGORY_AGG_SORT_OPS,
      paradigm = PARADIGM_DECLARATIVE,
      syntax = "%e:a until %e:b",
      description = "Defines an exclusive range of values",
      example = "'A' until 'G'",
      featureTitle = None
    ))
  }

  override def parseInstructionChain(stream: TokenStream, expr0: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    stream match {
      // inclusive range: e.g. "1 to 5"
      case ts if ts.nextIf("to") | ts.nextIf("..") =>
        val expr1 = compiler.nextExpression(ts).getOrElse(dieExpectedExpression())
        Some(Span.Inclusive(expr0, expr1))
      // exclusive range: e.g. "1 until 5"
      case ts if ts.nextIf("until") | ts.nextIf("..=") =>
        val expr1 = compiler.nextExpression(ts).getOrElse(dieExpectedExpression())
        Some(Span.Exclusive(expr0, expr1))
    }
  }

  def unapply(span: Span): Option[(Expression, Expression)] = Some((span.start, span.end))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    (ts is "to") || (ts is "until") || (ts is "..") || (ts is "..=")
  }

  private def evaluateRange(start: Expression, end: Expression)(implicit scope: Scope): Option[(Any, Any, DataType)] = {
    val (sa, ca, va) = start.execute(scope)
    val (sb, cb, vb) = end.execute(sa)
    for {a <- Option(va); b <- Option(vb)} yield {
      val (typeA, typeB) = (fromValue(a), fromValue(b))
      assert(typeA == typeB, "type mismatch")
      (a, b, typeA)
    }
  }

  /**
   * Represents an exclusive range-based array comprehension (e.g. ['A' until 'Z'])
   * @param start the start of the range
   * @param end   the exclusive end of the range
   */
  case class Exclusive(start: Expression, end: Expression) extends Span {

    override def execute()(implicit scope: Scope): (Scope, IOCost, Array[_]) = {
      val result = (for {(a, b, typeA) <- evaluateRange(start, end)} yield {
        typeA match {
          case CharType => (CharType.convert(a) until CharType.convert(b)).toArray
          case Int8Type => (Int8Type.convert(a) until Int8Type.convert(b)).toArray
          case Int16Type => (Int16Type.convert(a) until Int16Type.convert(b)).toArray
          case Int32Type => (Int32Type.convert(a) until Int32Type.convert(b)).toArray
          case Int64Type => (Int64Type.convert(a) until Int64Type.convert(b)).toArray
          case _ => dieUnsupportedType(typeA.name)
        }
      }).orNull
      (scope, IOCost.empty, result)
    }

    override def toSQL: String = s"${start.toSQL} until ${end.toSQL}"
  }

  /**
   * Represents an inclusive range-based array comprehension (e.g. ['A' to 'Z'])
   * @param start the start of the range
   * @param end   the inclusive end of the range
   */
  case class Inclusive(start: Expression, end: Expression) extends Span {

    override def execute()(implicit scope: Scope): (Scope, IOCost, Array[_]) = {
      val result = (for {(a, b, typeA) <- evaluateRange(start, end)} yield {
        typeA match {
          case CharType => (CharType.convert(a) to CharType.convert(b)).toArray
          case Int8Type => (Int8Type.convert(a) to Int8Type.convert(b)).toArray
          case Int16Type => (Int16Type.convert(a) to Int16Type.convert(b)).toArray
          case Int32Type => (Int32Type.convert(a) to Int32Type.convert(b)).toArray
          case Int64Type => (Int64Type.convert(a) to Int64Type.convert(b)).toArray
          case _ => dieUnsupportedType(typeA.name)
        }
      }).orNull
      (scope, IOCost.empty, result)
    }

    override def toSQL: String = s"${start.toSQL} to ${end.toSQL}"
  }

}