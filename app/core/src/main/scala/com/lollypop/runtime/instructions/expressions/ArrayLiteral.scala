package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.{ArrayExpression, Expression, Literal}
import com.lollypop.runtime.datatypes.{ArrayType, DataType, Inferences}
import com.lollypop.runtime.plastics.Tuples.seqToArray
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Represents an array literal
 * @param value the values within the array
 */
case class ArrayLiteral(value: List[Expression]) extends Literal
  with RuntimeExpression with ArrayExpression {
  private lazy val _type = ArrayType(Inferences.resolveType(value.map(Inferences.inferType)), capacity = Some(value.size))

  override def execute()(implicit scope: Scope): (Scope, IOCost, Array[_]) = {
    val (s, c, values) = value.transform(scope)
    (s, c, seqToArray(values))
  }

  override def returnType: DataType = _type

  override def toSQL: String = value.map(_.toSQL).mkString("[", ", ", "]")

}

object ArrayLiteral {
  def apply(values: Expression*): ArrayLiteral = {
    new ArrayLiteral(values.toList)
  }
}