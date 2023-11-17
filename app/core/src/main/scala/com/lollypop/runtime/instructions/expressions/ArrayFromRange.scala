package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.dieUnsupportedType
import com.lollypop.language.models.{ArrayExpression, Expression}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.Inferences.fromValue
import com.lollypop.runtime.datatypes._
import lollypop.io.IOCost

/**
 * Represents a range-based array comprehension
 */
sealed trait ArrayFromRange extends ArrayExpression with RuntimeExpression {

  def start: Expression

  def end: Expression

}

object ArrayFromRange {

  def unapply(r: ArrayFromRange): Option[(Expression, Expression)] = Some((r.start, r.end))

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
   * Represents an inclusive range-based array comprehension (e.g. ['A' to 'Z'])
   * @param start the start of the range
   * @param end   the inclusive end of the range
   */
  case class Inclusive(start: Expression, end: Expression) extends ArrayFromRange {

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

    override def toSQL: String = s"[${start.toSQL} to ${end.toSQL}]"
  }

  /**
   * Represents an exclusive range-based array comprehension (e.g. ['A' until 'Z'])
   * @param start the start of the range
   * @param end   the exclusive end of the range
   */
  case class Exclusive(start: Expression, end: Expression) extends ArrayFromRange {

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

    override def toSQL: String = s"[${start.toSQL} until ${end.toSQL}]"
  }

}