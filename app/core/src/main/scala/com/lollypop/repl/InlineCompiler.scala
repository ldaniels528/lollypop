package com.lollypop.repl

import com.lollypop.repl.LollypopREPL._
import com.lollypop.runtime.ModelStringRenderer.ModelStringRendering
import com.lollypop.runtime.Scope

import scala.annotation.tailrec

/**
 * Lollypop Inline Compiler
 */
trait InlineCompiler {

  /**
   * Facilitates the client conversation
   * @param scope      the [[Scope scope]]
   * @param console    the console input function
   * @param showPrompt the prompt display function
   * @return the updated [[Scope scope]]
   */
  @tailrec
  final def compileOnly(scope: Scope, console: () => String, showPrompt: () => Unit): Scope = {
    showPrompt()
    readFromConsole(console) match {
      // ignore blank lines
      case "" => compileOnly(scope, console, showPrompt)
      // quit the shell?
      case "exit" => scope
      // execute a complete statement?
      case sql =>
        val model = scope.getCompiler.compile(sql)
        Console.println(model.asModelString)
        compileOnly(scope, console, showPrompt)
    }
  }

}