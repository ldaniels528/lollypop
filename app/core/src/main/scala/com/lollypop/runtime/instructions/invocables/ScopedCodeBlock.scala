package com.lollypop.runtime.instructions.invocables

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.models.CodeBlock.summarize
import com.lollypop.language.models.{CodeBlock, Instruction}
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

/**
 * Represents an scope-isolated code block
 * @param instructions one or more [[Instruction instructions]] to execute
 * @example {{{
 * {
 *    val x = 1
 *    val y = 2
 *    val z = 3
 *    x + y + z
 * }
 * }}}
 */
case class ScopedCodeBlock(instructions: List[Instruction]) extends CodeBlock with RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val scope0 = Scope(scope)
    summarize(scope0, instructions) ~> { case (_, c, r) => (scope, c, r) }
  }
}

/**
 * Scoped Code Block Companion
 * @author lawrence.daniels@gmail.com
 */
object ScopedCodeBlock {

  /**
   * Returns an SQL code block containing the given operations
   * @param operations the given collection of [[Instruction]]
   * @return the [[CodeBlock code block]]
   */
  def apply(operations: Instruction*): ScopedCodeBlock = new ScopedCodeBlock(operations.toList)

}
