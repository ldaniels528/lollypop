package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_IMPERATIVE}
import com.lollypop.language._
import com.lollypop.language.models.TypicalFunction
import com.lollypop.runtime.DatabaseManagementSystem.createDurableFunction
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

/**
 * create function statement
 * @param ref         the [[DatabaseObjectRef persistent object reference]]
 * @param function    the given [[TypicalFunction durable function]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateFunction(ref: DatabaseObjectRef, function: TypicalFunction, ifNotExists: Boolean)
  extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = createDurableFunction(ref.toNS, function, ifNotExists)
    (scope, cost, cost)
  }

  override def toSQL: String = {
    ("create function" :: (if (ifNotExists) List("if not exists") else Nil) :::
      s"${ref.toSQL}(${function.params.map(_.toSQL).mkString(",")}) := ${function.code.toSQL}" :: Nil).mkString(" ")
  }

}

object CreateFunction extends ModifiableParser with IfNotExists {
  val templateCard: String =
    """|create function ?%IFNE:exists %L:name ?%FP:params %C(_|:=|as) %i:code
       |""".stripMargin

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "create function",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "Creates a function",
    example = "create function if not exists calc_add(a: Int, b: Int) := a + b"
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[CreateFunction] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      Some(CreateFunction(ref = params.locations("name"),
        TypicalFunction(
          params = params.parameters.getOrElse("params", Nil),
          code = params.instructions("code")
        ), ifNotExists = params.indicators.get("exists").contains(true)))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "create function"

}