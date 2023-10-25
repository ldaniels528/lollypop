package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_SESSION, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Atom
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.Scope
import qwery.io.IOCost

/**
 * Declare Package
 * @param packageName the new default package name
 */
case class DeclarePackage(packageName: Atom) extends RuntimeInvokable {

  override def invoke()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope.withVariable("__package__", packageName.name), IOCost.empty, packageName.name)
  }

  override def toSQL: String = s"package ${packageName.toSQL}"

}

object DeclarePackage extends InvokableParser {
  private val templateCard = "package %a:packageName"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "package",
    category = CATEGORY_SESSION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Declares the default JVM package namespace",
    example =
      """|package "com.acme.skunkworks"
         |__package__
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): DeclarePackage = {
    val params = SQLTemplateParams(ts, templateCard)
    DeclarePackage(params.atoms("packageName"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "package"

}