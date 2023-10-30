package com.lollypop.language.models

import com.lollypop.language.Token

import java.io.File

/**
 * SourceCode Instruction
 */
trait SourceCodeInstruction extends Instruction {
  private var sourceFile: Option[File] = None
  private var lineNumber: Option[Int] = None
  private var columnNumber: Option[Int] = None

  def setFile(file: File): this.type = {
    if (sourceFile.isEmpty) {
      this.sourceFile = Some(file)
    }
    this
  }

  def setLocation(token: Option[Token]): this.type = {
    if (lineNumber.isEmpty) lineNumber = token.map(_.lineNo)
    if (columnNumber.isEmpty) columnNumber = token.map(_.columnNo)
    this
  }

  override def toMessage: String = {
    (sourceFile.map(file => s"in '${file.getPath}'").toList :::
      lineNumber.map(line => s"on line $line").toList :::
      columnNumber.map(column => s"at $column").toList).mkString(" ")
  }

}

/**
 * SourceCode Instruction
 */
object SourceCodeInstruction {

  final implicit class RichSourceCodeInstruction[T <: Instruction](val instruction: T) extends AnyVal {

    @inline
    def updateFile(file: File): T = {
      def recurse(ins: Instruction): Unit = ins match {
        case op@CodeBlock(ops) =>
          op.setFile(file)
          ops.foreach(recurse)
        case si: SourceCodeInstruction => si.setFile(file)
        case _ =>
      }

      recurse(instruction)
      instruction
    }

    @inline
    def updateLocation(token: Option[Token]): T = {
      def recurse(ins: Instruction): Unit = ins match {
        case op@CodeBlock(ops) =>
          op.setLocation(token)
          ops.foreach(recurse)
        case si: SourceCodeInstruction => si.setLocation(token)
        case _ =>
      }

      recurse(instruction)
      instruction
    }

  }

}