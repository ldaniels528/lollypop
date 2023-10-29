package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.plastics.RuntimeClass
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

/**
 * Import Implicit Class
 * @param expression the implicit class name
 */
case class ImportImplicitClass(expression: Expression) extends RuntimeInvokable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val s = scope.importImplicitClass(RuntimeClass.getClassByName(expression.asString || expression.dieIllegalType()))
    (s, IOCost.empty, null)
  }

  override def toSQL: String = List("import implicit", expression.toSQL).mkString(" ")
}

/**
 * Import Implicit Class Companion
 */
object ImportImplicitClass extends InvokableParser {
  val templateCard = "import implicit %e:target"

  override def help: List[HelpDoc] = {
    import com.qwery.util.OptionHelper.implicits.risky._
    List(HelpDoc(
      name = "import implicit",
      category = CATEGORY_SYSTEM_TOOLS,
      paradigm = PARADIGM_OBJECT_ORIENTED,
      syntax = templateCard,
      featureTitle = "Import (Scala-compiled) Implicit Classes",
      description = "Imports the methods of a Scala implicit class",
      example =
        """|import implicit "com.qwery.util.StringRenderHelper$StringRenderer"
           |DateTime().renderAsJson()
           |""".stripMargin
    ))
  }

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): ImportImplicitClass = {
    val params = SQLTemplateParams(ts, templateCard)
    ImportImplicitClass(params.expressions("target"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "import implicit"
}