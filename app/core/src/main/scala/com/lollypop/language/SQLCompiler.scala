package com.lollypop.language

import com.lollypop.language.TemplateProcessor.TagInstructionWithLineNumbers
import com.lollypop.language.models._
import com.lollypop.runtime.instructions.MacroLanguageParser
import com.lollypop.runtime.instructions.invocables.{InlineCodeBlock, InstructionChain}
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper.AutoClose

import java.io.{File, FileInputStream, InputStream}
import scala.io.Source
import scala.language.{existentials, postfixOps}

/**
 * SQL Compiler
 * @author lawrence.daniels@gmail.com
 */
trait SQLCompiler extends TemplateProcessor {
  protected implicit val _compiler: SQLCompiler = this

  /**
   * @return the [[LollypopUniverse compiler context]]
   */
  def ctx: LollypopUniverse

  /**
   * Parses the contents of the given file into an [[Instruction Instruction]]
   * @param file the [[File file]] to compile
   * @return the resultant [[Instruction]]
   */
  def compile(file: File): Instruction = new FileInputStream(file).use(compile)

  /**
   * Parses the contents of the given input stream into an [[Instruction Instruction]]
   * @param stream the given [[InputStream input stream]]
   * @return the resultant [[Instruction]]
   */
  def compile(stream: InputStream): Instruction = compile(Source.fromInputStream(stream).mkString)

  /**
   * Parses the contents of the given string into an [[Instruction instruction]]
   * @param sourceCode the given SQL code string (e.g. "select * from Customers")
   * @return the resultant [[Instruction]]
   */
  def compile(sourceCode: String): Instruction = {
    iterate(TokenStream(sourceCode)).toList match {
      case op :: Nil => op
      case ops => InlineCodeBlock(ops)
    }
  }

  /**
   * Returns an iterator of executables
   * @param ts the given [[TokenStream token stream]]
   * @return an iterator of [[Instruction]]s
   */
  private def iterate(ts: TokenStream): Iterator[Instruction] = new Iterator[Instruction] {
    override def hasNext: Boolean = ts.hasNext

    override def next(): Instruction = processCompilerInstruction(ts) || nextOpCodeOrDie(ts)
  }

  override def nextOpCode(ts: TokenStream): Option[Instruction] = {
    val t0 = ts.peek
    //logger.info(s"[L${t0.map(_.lineNo).getOrElse("?")}:C${t0.map(_.columnNo).getOrElse("?")}] '${t0.map(_.text).orNull}'")
    (ctx.getStatement(ts) ?? nextCodeBlockOrDictionary(ts) ?? ctx.getModifiable(ts) ?? nextExpression(ts) ??
      MacroLanguageParser(ts)).map(_.tag(t0)).map(InstructionChain(ts, _))
  }

  private def processCompilerInstruction(stream: TokenStream): Option[Instruction] = ctx.getDirective(stream)

}
