package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_IMPERATIVE}
import com.qwery.language.models.{Expression, FieldRef, VariableRef}
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream, dieUnsupportedType}
import com.qwery.runtime.Scope
import qwery.io.IOCost

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