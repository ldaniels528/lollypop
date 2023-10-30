package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language.models.Expression
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.datatypes.AnyType
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.plastics.RuntimeClass
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import scala.annotation.tailrec

/**
 * Import Statement
 * @example {{{
 *   import 'java.util.Date'
 *   import JDate: 'java.util.Date'
 *   import ['java.util.Date', 'java.sql.DateTime']
 * }}}
 */
case class Import(target: Expression) extends RuntimeInvokable {

  override def execute()(implicit scope0: Scope): (Scope, IOCost, Any) = {
    @tailrec
    def recurse(scope: Scope, value: Any): (Scope, Any) = value match {
      case null => scope -> null
      case array: Array[_] => recurse(scope, Option(array).map(_.toList).orNull)
      case coll: Seq[_] =>
        val classNames = coll.map { case s: String => s; case x => dieIllegalType(x) }
        classNames.foldLeft[Scope](scope) { (agg, className) => importClass(agg, className) } -> value
      case className: String if className.nonEmpty => importClass(scope, className) -> value
      case x => dieIllegalType(x)
    }

    val (scope1, cost1, result1) = LollypopVM.execute(scope0, target)
    val (scope2, result2) = recurse(scope1, result1)
    (scope2, cost1, result2)
  }

  private def importClass(scope: Scope, classQName: String): Scope = {
    val _class = RuntimeClass.getClassByName(classQName)(scope)
    val localName = target.getAlias || _class.getSimpleName
    scope
      .withVariable(localName, `type` = AnyType(className = classQName), value = _class, isReadOnly = false)
      .withImports(Map(localName -> classQName))
  }

  override def toSQL: String = {
    ("import" :: target.getAlias.map(s => s"$s:").toList ::: target.toSQL :: Nil).mkString(" ")
  }
}

/**
 * Import Companion
 */
object Import extends InvokableParser {
  val templateCard = "import %e:target"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "import",
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_OBJECT_ORIENTED,
    syntax = templateCard,
    description = "Imports a JVM class",
    example = "import 'java.util.Date'"
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Import = {
    val params = SQLTemplateParams(ts, templateCard)
    Import(target = params.expressions("target"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    (ts is "import") && (ts isnt "import implicit")
  }
}