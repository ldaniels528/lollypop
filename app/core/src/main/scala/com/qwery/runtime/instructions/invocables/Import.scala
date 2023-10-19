package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_SYSTEMS, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.datatypes.AnyType
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.{QweryVM, RuntimeClass, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

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

  override def invoke()(implicit scope0: Scope): (Scope, IOCost, Any) = {
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

    val (scope1, cost1, result1) = QweryVM.execute(scope0, target)
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
    category = CATEGORY_SYSTEMS,
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