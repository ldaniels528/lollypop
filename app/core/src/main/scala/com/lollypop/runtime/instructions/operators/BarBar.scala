package com.lollypop.runtime.instructions.operators

import com.lollypop.language.models.{BinaryOperation, Expression}
import com.lollypop.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.operators.ComputeAndSet.ComputeAndSetSugar

/**
 * || operator
 */
case class BarBar(a: Expression, b: Expression) extends BinaryOperation {

  override def operator: String = "||"

}

object BarBar extends ExpressionChainParser {

  override def help: List[HelpDoc] = Nil

  override def parseExpressionChain(stream: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    stream match {
      case ts if ts nextIf "||" => for (b <- compiler.nextExpression(ts)) yield BarBar(host, b)
      case ts if ts nextIf "||=" => for (b <- compiler.nextExpression(ts)) yield BarBar(host, b).doAndSet
      case _ => None
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = Seq("||", "||=").exists(ts is _)

}
