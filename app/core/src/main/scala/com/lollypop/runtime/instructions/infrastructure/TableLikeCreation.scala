package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.runtime.DatabaseObjectRef

/**
 * Table-Like Creation Trait
 */
trait TableLikeCreation extends TableCreation {

  /**
   * @return the source/template table [[DatabaseObjectRef reference]]
   */
  def template: DatabaseObjectRef

  override def toSQL: String = {
    (actionVerb :: "table" :: (if (ifNotExists) List("if not exists") else Nil) :::
      ref.toSQL :: "like" :: template.toSQL :: "(" :: tableModel.columns.map(_.toSQL).mkString(",") ::
      ")" :: Nil).mkString(" ")
  }

}
