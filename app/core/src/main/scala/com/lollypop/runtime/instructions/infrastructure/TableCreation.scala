package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.{Instruction, TableModel}

/**
 * Table Creation Trait
 */
trait TableCreation extends RuntimeModifiable {

  /**
   * @return the [[Instruction reference]]
   */
  def ref: Instruction

  /**
   * @return the [[TableModel table model]]
   */
  def tableModel: TableModel

  /**
   * @return if true, the operation will not fail when the entity exists
   */
  def ifNotExists: Boolean

  /**
   * @return the action verb (e.g. "create" or "declare")
   */
  protected def actionVerb: String

  override def toSQL: String = {
    (actionVerb :: "table" :: (if (ifNotExists) List("if not exists") else Nil) ::: ref.toSQL ::
      "(" :: tableModel.columns.map(_.toSQL).mkString(",") :: ")" :: Nil).mkString(" ")
  }

}
