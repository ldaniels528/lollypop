package com.qwery.runtime.instructions.invocables

import com.qwery.implicits.MagicImplicits
import com.qwery.language.models.CodeBlock.summarize
import com.qwery.language.models.{CodeBlock, Instruction}
import com.qwery.runtime.Scope
import qwery.io.IOCost

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

  override def invoke()(implicit scope: Scope): (Scope, IOCost, Any) = {
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
