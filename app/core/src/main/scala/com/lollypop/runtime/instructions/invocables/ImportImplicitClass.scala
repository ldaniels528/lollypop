package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language.models.Expression
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.plastics.RuntimeClass
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

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
    import com.lollypop.util.OptionHelper.implicits.risky._
    List(HelpDoc(
      name = "import implicit",
      category = CATEGORY_SYSTEM_TOOLS,
      paradigm = PARADIGM_OBJECT_ORIENTED,
      syntax = templateCard,
      featureTitle = "Implicit Class Importing",
      description = "Imports the methods of a Scala implicit class",
      example =
        """|import implicit "com.lollypop.util.StringRenderHelper$StringRenderer"
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