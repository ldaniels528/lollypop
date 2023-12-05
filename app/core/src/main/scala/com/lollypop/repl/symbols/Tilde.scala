package com.lollypop.repl.symbols

import com.lollypop.repl.symbols.Tilde.symbol

import scala.util.Properties

/**
 * Tilde symbol (~)
 * @example cd ~
 * @example ls ~
 */
case class Tilde() extends REPLSymbol(symbol, _ => Properties.userHome)

object Tilde extends REPLSymbolParser(symbol = "~", () => new Tilde())