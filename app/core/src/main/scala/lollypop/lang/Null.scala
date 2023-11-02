package lollypop.lang

import com.lollypop.language.models.{Expression, Literal}
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.{AnyType, DataType}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import lollypop.io.IOCost

/**
 * Represents a Null value or expression
 */
case class Null(dataType: DataType = AnyType) extends RuntimeExpression with Literal {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope, IOCost.empty, null)
  }

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