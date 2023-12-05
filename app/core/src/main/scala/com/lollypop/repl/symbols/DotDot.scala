package com.lollypop.repl.symbols

import com.lollypop.repl.symbols.DotDot.symbol
import com.lollypop.runtime.conversions.getCWD

import java.io.File

/**
 * Dot-Dot symbol (..)
 * @example {{{
 *  cd ..
 * }}}
 */
case class DotDot() extends REPLSymbol(symbol, sc => new File(getCWD(sc)).getParent)

object DotDot extends REPLSymbolParser(symbol = "..", () => new DotDot())