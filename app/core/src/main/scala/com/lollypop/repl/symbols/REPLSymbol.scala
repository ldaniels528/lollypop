package com.lollypop.repl.symbols

import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import lollypop.io.IOCost

/**
 * REPL Symbol
 * @param symbol the symbol character (e.g. "~")
 * @param f      the transformation function
 */
abstract class REPLSymbol(val symbol: String, f: Scope => String) extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, String) = {
    (scope, IOCost.empty, f(scope))
  }

  override def toSQL: String = symbol

}

