package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.runtime.DatabaseManagementSystem.createIndex
import com.lollypop.runtime.instructions.ReferenceInstruction
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

/**
 * create index statement
 * @param ref         the host [[DatabaseObjectRef host table reference]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateIndex(ref: DatabaseObjectRef, ifNotExists: Boolean)
  extends ReferenceInstruction with RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = IOCost(created = 1) ++ createIndex(ref.toNS, ifNotExists)
    (scope, cost, cost)
  }

  override def toSQL: String = {
    ("create index" :: (if (ifNotExists) List("if not exists") else Nil) ::: ref.toSQL :: Nil).mkString(" ")
  }

}

object CreateIndex extends ModifiableParser with IfNotExists {
  private val template = "create index ?%IFNE:exists %L:table"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "create index",
    category = CATEGORY_DATAFRAMES_INFRA,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Creates a table index",
    example =
      """|create index if not exists stocks#symbol
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[CreateIndex] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Some(CreateIndex(ref = params.locations("table"), ifNotExists = params.indicators.get("exists").contains(true)))
    } else None
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "create index"

}
