package com.qwery.runtime.instructions.expressions

import com.qwery.language.dieUnsupportedType
import com.qwery.language.models.{ArrayExpression, Expression}
import com.qwery.runtime.datatypes.Inferences.fromValue
import com.qwery.runtime.datatypes._
import com.qwery.runtime.{QweryVM, Scope}

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
    for {a <- Option(QweryVM.execute(scope, start)._3); b <- Option(QweryVM.execute(scope, end)._3)} yield {
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

    override def evaluate()(implicit scope: Scope): Array[_] = {
      (for {(a, b, typeA) <- evaluateRange(start, end)} yield {
        typeA match {
          case CharType => (CharType.convert(a) to CharType.convert(b)).toArray
          case Int8Type => (Int8Type.convert(a) to Int8Type.convert(b)).toArray
          case Int16Type => (Int16Type.convert(a) to Int16Type.convert(b)).toArray
          case Int32Type => (Int32Type.convert(a) to Int32Type.convert(b)).toArray
          case Int64Type => (Int64Type.convert(a) to Int64Type.convert(b)).toArray
          case _ => dieUnsupportedType(typeA.name)
        }
      }).orNull
    }

    override def toSQL: String = s"[${start.toSQL} to ${end.toSQL}]"
  }

  /**
   * Represents an exclusive range-based array comprehension (e.g. ['A' until 'Z'])
   * @param start the start of the range
   * @param end   the exclusive end of the range
   */
  case class Exclusive(start: Expression, end: Expression) extends ArrayFromRange {

    override def evaluate()(implicit scope: Scope): Array[_] = {
      (for {(a, b, typeA) <- evaluateRange(start, end)} yield {
        typeA match {
          case CharType => (CharType.convert(a) until CharType.convert(b)).toArray
          case Int8Type => (Int8Type.convert(a) until Int8Type.convert(b)).toArray
          case Int16Type => (Int16Type.convert(a) until Int16Type.convert(b)).toArray
          case Int32Type => (Int32Type.convert(a) until Int32Type.convert(b)).toArray
          case Int64Type => (Int64Type.convert(a) until Int64Type.convert(b)).toArray
          case _ => dieUnsupportedType(typeA.name)
        }
      }).orNull
    }

    override def toSQL: String = s"[${start.toSQL} until ${end.toSQL}]"
  }

}