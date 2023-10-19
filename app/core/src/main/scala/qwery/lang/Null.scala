package qwery.lang

import com.qwery.language.models.{Expression, Literal}
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.{AnyType, DataType}
import com.qwery.runtime.instructions.expressions.RuntimeExpression

/**
 * Represents a Null value or expression
 */
case class Null(dataType: DataType = AnyType) extends RuntimeExpression with Literal {

  override def evaluate()(implicit scope: Scope): Any = null

  override def returnType: DataType = dataType

  override def toSQL: String = "null"

  override val value: Any = null

}

object Null extends ExpressionParser {

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts.nextIf("null")) Some(Null()) else None
  }

  override def help: List[HelpDoc] = Nil

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "null"

}