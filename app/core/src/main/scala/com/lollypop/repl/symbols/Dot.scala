package com.lollypop.repl.symbols

import com.lollypop.repl.symbols.Dot.symbol
import com.lollypop.runtime._

/**
 * Dot symbol (.)
 * @example {{{
 *  ls .
 * }}}
 */
case class Dot() extends REPLSymbol(symbol, sc => getCWD(sc))

object Dot extends REPLSymbolParser(symbol = ".", () => new Dot())
