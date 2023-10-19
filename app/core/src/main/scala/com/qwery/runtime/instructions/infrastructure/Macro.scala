package com.qwery.runtime.instructions.infrastructure

import com.qwery.die
import com.qwery.implicits.MagicImplicits
import com.qwery.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Instruction
import com.qwery.language.{HelpDoc, ModifiableParser, SQLCompiler, SQLTemplateParams, TokenStream, dieIllegalType}
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.MacroLanguageParser
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

case class Macro(template: String, code: Instruction) extends RuntimeModifiable {

  override def invoke()(implicit scope: Scope): (Scope, IOCost) = {
    MacroLanguageParser.registerMacro(this)
    scope -> IOCost(created = 1)
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
         |out <=== drawCircle(100)@(80, 650)
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Macro = {
    val params = SQLTemplateParams(ts, template)
    parseMacro(params)
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
    val macroTemplate = (params.expressions("template") ~> { e => e.asString || dieIllegalType(e) }).trim
    if (!macroTemplate.headOption.exists(identifierChar)) die(s"Invalid macro template '$macroTemplate' must begin with an identifier")
    macroTemplate
  }

  private val identifierChar: Char => Boolean = { c => c.isLetter || c == '_' || c >= 128 }

}