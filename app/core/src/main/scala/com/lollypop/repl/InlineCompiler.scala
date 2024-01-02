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
  def compileOnly(scope: Scope, console: () => String, showPrompt: () => Unit): Scope = {
    @tailrec
    def recurse(blankLines: Int): Scope = {
      showPrompt()
      readFromConsole(console) match {
        // quit the routine after 2 blank lines
        case "" => if (blankLines == 1) scope else recurse(blankLines = blankLines + 1)
        // execute a complete statement?
        case sql =>
          val model = scope.getCompiler.compile(sql)
          Console.println(model.asModelString)
          recurse(blankLines = 0)
      }
    }

    recurse(blankLines = 0)
  }

}