package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Atom
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

/**
 * Declare Package
 * @param packageName the new default package name
 */
case class DeclarePackage(packageName: Atom) extends RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope.withVariable("__package__", packageName.name), IOCost.empty, packageName.name)
  }

  override def toSQL: String = s"package ${packageName.toSQL}"

}

object DeclarePackage extends InvokableParser {
  private val templateCard = "package %a:packageName"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "package",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Declares the default JVM package namespace",
    isExperimental = true,
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