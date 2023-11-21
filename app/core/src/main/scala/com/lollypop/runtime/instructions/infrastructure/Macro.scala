package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.die
import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Instruction
import com.lollypop.language.{HelpDoc, ModifiableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import com.lollypop.runtime.instructions.MacroLanguageParser
import lollypop.io.IOCost

case class Macro(template: String, code: Instruction) extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    MacroLanguageParser.registerMacro(this)
    val cost = IOCost(created = 1)
    (scope, cost, cost)
  }

  override def toSQL: String = List("macro", s"\"$template\"", ":=", code.toSQL).mkString(" ")
}

object Macro extends ModifiableParser {
  private[infrastructure] val template = "macro %e:template %C(_|as|:=) %i:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "macro",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Creates a user-defined instruction",
    example =
      """|macro "drawCircle ( %e:size ) @ ( %e:x , %e:y )" := {
         |  "Circle({{size}}) <- ({{x * 2}}, {{y / 2}})"
         |}
         |
         |stdout <=== drawCircle(100)@(80, 650)
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Macro] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Some(parseMacro(params))
    } else None
  }

  def parseMacro(params: SQLTemplateParams)(implicit compiler: SQLCompiler): Macro = {
    val macroTemplate = decodeTemplate(params)
    val model = Macro(macroTemplate, code = params.instructions("code"))
    compiler.ctx.addMacro(model)
    model
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "macro"

  private def decodeTemplate(params: SQLTemplateParams): String = {
    implicit val scope: Scope = Scope()
    val macroTemplate = params.expressions("template").pullString._3.trim
    if (!macroTemplate.headOption.exists(identifierChar)) die(s"Invalid macro template '$macroTemplate' must begin with an identifier")
    macroTemplate
  }

  private val identifierChar: Char => Boolean = { c => c.isLetter || c == '_' || c >= 128 }

}