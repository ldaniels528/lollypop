package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_IMPERATIVE}
import com.lollypop.language.models.{Expression, FieldRef, VariableRef}
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream, dieUnsupportedType}
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

case class Destroy(ref: Expression) extends RuntimeInvokable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val s = ref match {
      case VariableRef(name) => scope.removeVariable(name)
      case FieldRef(name) => scope.removeVariable(name)
      case other => dieUnsupportedType(other)
    }
    (s, IOCost.empty, null)
  }

  override def toSQL: String = s"destroy ${ref.toSQL}"
}

object Destroy extends InvokableParser {
  private val templateCard = "destroy %e:name"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "destroy",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "Removes a variable from the active scope",
    example = "destroy stocks"
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Destroy = {
    val params = SQLTemplateParams(ts, templateCard)
    Destroy(ref = params.expressions("name"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "destroy"
}