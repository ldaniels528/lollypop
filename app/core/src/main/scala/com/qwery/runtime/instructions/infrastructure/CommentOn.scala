package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.{HelpDoc, IfNotExists, ModifiableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.DatabaseManagementSystem.commentOn
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.{DatabaseObjectRef, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

/**
 * Sets remarks (comments) on a database object
 * @param ref      the [[DatabaseObjectRef]]
 * @param remarks  the comments/remarks
 * @param ifExists indicates an error should not occur if the object does not exist
 */
case class CommentOn(ref: DatabaseObjectRef, remarks: Expression, ifExists: Boolean) extends RuntimeModifiable {

  override def invoke()(implicit scope: Scope): (Scope, IOCost) = {
    scope -> commentOn(ns = ref.toNS, remarks = remarks.asString || "", ifExists)
  }

  override def toSQL: String = {
    ("comment on" :: (if (ifExists) List("if exists") else Nil) ::: ref.toSQL :: ":=" :: remarks.toSQL :: Nil).mkString(" ")
  }

}

object CommentOn extends ModifiableParser with IfNotExists {
  private val template = "comment on ?%IFE:exists %L:ref %C(_|:=|as) %e:remarks"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "comment",
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Sets remarks (comments) on a database object",
    example = "comment on if exists stocks := 'just a staging table'"
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): CommentOn = {
    val params = SQLTemplateParams(ts, template)
    CommentOn(
      ref = params.locations("ref"),
      remarks = params.expressions("remarks"),
      ifExists = params.indicators.get("exists").contains(true))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "comment on"

}

