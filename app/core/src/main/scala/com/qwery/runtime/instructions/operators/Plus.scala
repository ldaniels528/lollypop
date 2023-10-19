package com.qwery.runtime.instructions.operators

import com.qwery.language.models.{Expression, BinaryOperation}
import com.qwery.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.instructions.operators.ComputeAndSet.ComputeAndSetSugar

case class Plus(a: Expression, b: Expression) extends BinaryOperation {

  override def operator: String = "+"

}

object Plus extends ExpressionChainParser {

  override def help: List[HelpDoc] = Nil

  override def parseExpressionChain(stream: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    stream match {
      case ts if ts nextIf "+" => for (b <- compiler.nextExpression(ts)) yield Plus(host, b)
      case ts if ts nextIf "+=" => for (b <- compiler.nextExpression(ts)) yield Plus(host, b).doAndSet
      case _ => None
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = Seq("+", "+=").exists(ts is _)

}
