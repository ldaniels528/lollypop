package com.lollypop.repl.symbols

import com.lollypop.repl.symbols.Caret.symbol

/**
 * Caret symbol (`^`)
 */
case class Caret() extends REPLSymbol(symbol, _ => symbol)

object Caret extends REPLSymbolParser(symbol = "^", () => new Caret())
