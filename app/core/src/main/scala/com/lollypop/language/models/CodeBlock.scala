package com.lollypop.language.models

import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.invocables.ScopedCodeBlock
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

/**
 * Represents a code block
 */
trait CodeBlock extends Invokable with SourceCodeInstruction {

  /**
   * @return one or more [[Instruction instructions]] to execute
   */
  def instructions: List[Instruction]

  override def toSQL: String = ("{" :: instructions.map(i => s"  ${i.toSQL}") ::: "}" :: Nil).mkString("\n")

}

/**
 * Code Block Companion
 * @author lawrence.daniels@gmail.com
 */
object CodeBlock extends InvokableParser {

  /**
   * Returns an SQL code block containing the given operations
   * @param instructions the given collection of [[Instruction]]
   * @return the [[CodeBlock code block]]
   */
  def apply(instructions: List[Instruction]): CodeBlock = ScopedCodeBlock(instructions)

  /**
   * Returns an SQL code block containing the given operations
   * @param operations the given collection of [[Instruction]]
   * @return the [[CodeBlock code block]]
   */
  def apply(operations: Instruction*): CodeBlock = apply(operations.toList)

  def unapply(cb: CodeBlock): Option[List[Instruction]] = Some(cb.instructions)

  override def help: List[HelpDoc] = Nil

  override def parseInvokable(stream: TokenStream)(implicit compiler: SQLCompiler): CodeBlock = {
    stream match {
      case ts if ts is "begin" => parseSequence(ts, "begin", "end")
      case ts => ts.dieExpectedKeywords("begin")
    }
  }

  private def parseSequence[A <: CodeBlock](ts: TokenStream, start: String, end: String)(implicit compiler: SQLCompiler): CodeBlock = {
    var statements: List[Instruction] = Nil
    ts.expect(start)
    while (ts isnt end) statements = compiler.nextOpCodeOrDie(ts) :: statements
    ts.expect(end)
    CodeBlock(statements.reverse)
  }

  def summarize(scope0: Scope, instructions: List[Instruction]): (Scope, IOCost, Any) = {
    instructions.foldLeft[(Scope, IOCost, Any)]((scope0, IOCost.empty, None)) { case ((s0, c0, _), instruction) =>
      val (s1, c1, r1) = LollypopVM.execute(s0, instruction)
      (s1, c0 ++ c1, r1)
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "begin"

}