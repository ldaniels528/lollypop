package com.lollypop.language.models

import com.lollypop.runtime.DatabaseObjectRef
import com.lollypop.runtime.instructions.expressions.{ElementAt, New}
import com.lollypop.runtime.instructions.functions.AnonymousFunction
import com.lollypop.runtime.instructions.queryables.From

/**
 * Base class for all Virtual Machine instructions
 * @author lawrence.daniels@gmail.com
 */
trait Instruction extends InstructionErrors {

  def toMessage: String = ""

  def toSQL: String = ???

  def wrapSQL: String = wrapSQL(this.needsWrapper)

  def wrapSQL(required:  Boolean = false): String = if (required || this.needsWrapper) s"($toSQL)" else toSQL

}

object Instruction {

  /**
   * Decompiler Alias Helper
   * @param sql the SQL query or statement
   */
  final implicit class DecompilerAliasHelper(val sql: String) extends AnyVal {
    def withAlias(alias_? : Option[String]): String = alias_?.map(alias => s"$alias: $sql").getOrElse(sql)
  }

  /**
   * Decompiler Alias Helper
   * @param instruction the [[Instruction instruction]]
   */
  final implicit class RichInstruction(val instruction: Instruction) extends AnyVal {
    @inline def needsWrapper: Boolean = instruction match {
      case _: DatabaseObjectRef => false
      case _: Literal => false
      case _: IdentifierRef => false
      case _: AnonymousFunction => true
      case _: CodeBlock => true
      case _: Condition => true
      case _: ElementAt => true
      case _: New => true
      case _: Operation => true
      case q: Queryable => !q.isInstanceOf[From]
      case _ => false
    }
  }

}