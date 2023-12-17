package com.lollypop.runtime.instructions.operators

import com.lollypop.language._
import com.lollypop.language.models.{Expression, UnaryOperation}

/**
 * Represents a bang (!) function call
 */
case class Bang(a: Expression) extends UnaryOperation {

  override def operator: String = "!"

}

object Bang extends ExpressionParser {

  override def help: List[HelpDoc] = Nil

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Bang] = {
    if (ts nextIf "!") compiler.nextExpression(ts).map(Bang.apply) ?? ts.dieExpectedNumeric() else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = Seq("!", "!=").exists(ts is _)

}
