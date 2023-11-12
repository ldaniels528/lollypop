package com.lollypop.runtime

import com.lollypop.language.SQLCompiler
import com.lollypop.language.models.{CodeBlock, Instruction, Queryable}
import com.lollypop.runtime.LollypopCodeDebugger.QApplication
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.instructions.invocables.InlineCodeBlock
import com.lollypop.util.ResourceHelper._
import com.lollypop.util.StringRenderHelper.StringRenderer

import java.io.File
import scala.io.Source

/**
 * Simple Code Debugger for Lollypop
 */
trait LollypopCodeDebugger {

  def stepThrough(file: String, console: () => String)(implicit compiler: LollypopCompiler): Scope = {
    stepThrough(app = new QApplication(new File(file)), console)
  }

  def stepThrough(file: File, console: () => String)(implicit compiler: LollypopCompiler): Scope = {
    stepThrough(app = new QApplication(file), console)
  }

  def stepThrough(app: QApplication, console: () => String): Scope = {
    import scala.Console._
    var isDone: Boolean = false

    // display the first instruction
    app.showCode()

    // step through the code
    do {
      app.prompt()
      console().trim match {
        case "" => app.executeCode()
        case "l" | "list" => app.showFullCode()
        case "q" | "exit" => isDone = true
        case "p" | "page" => app.showPageOfCode()
        case "r" | "result" => app.showResult()
        case "s" | "show" => app.showCode()
        case "?" | "scope" => app.scope.toRowCollection.tabulate().foreach(println)
        case x => println(s"Unrecognized debugger command:\n$RED$x$RESET")
      }
    } while (!isDone)
    app.scope
  }

}

object LollypopCodeDebugger {

  import scala.Console._

  def apply(): LollypopCodeDebugger = {
    new LollypopCodeDebugger {}
  }

  class QApplication(file: File, initialScope: Scope = Scope())(implicit compiler: SQLCompiler) {
    private val instructions: List[Instruction] = loadApplication(file)
    var scope: Scope = initialScope
    var result: Any = _
    var position: Int = 0
    var pageSize: Int = 8

    def executeCode(): Unit = {
      getInstruction foreach { op =>
        val (scope0, _, result0) = op.execute(scope)
        scope = scope0
        result = result0
        position += 1
        showCode()
      }
    }

    def prompt(): Unit = print(f"$position%03d> ")

    def showCode(offset: Int = position): Unit = getInstruction(offset) foreach {
      case instruction: Queryable =>
        println(f"[$offset%03d] $MAGENTA${instruction.toSQL}$RESET")
      case instruction =>
        println(f"[$offset%03d] $GREEN${instruction.toSQL}$RESET")
    }

    def showFullCode(): Unit = for (offset <- instructions.indices) showCode(offset)

    def showPageOfCode(): Unit = getPageOffsets.foreach(showCode)

    def showResult(): Unit = println(s"result: ${result.render}")

    private def getInstruction: Option[Instruction] = getInstruction(position)

    private def getInstruction(offset: Int): Option[Instruction] = {
      if (offset < instructions.size) Option(instructions(offset)) else None
    }

    private def getPageOffsets: Seq[Int] = {
      for (offset <- position to ((position + pageSize) min instructions.length)) yield offset
    }

    private def loadApplication(file: File)(implicit compiler: SQLCompiler): List[Instruction] = {
      val sql = Source.fromFile(file).use(_.mkString)
      compiler.compile(sql) match {
        case InlineCodeBlock(statements) => statements
        case CodeBlock(statements) => statements
        case code => List(code)
      }
    }

  }

}