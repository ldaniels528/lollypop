package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language._
import com.lollypop.language.models.Expression
import com.lollypop.runtime.datatypes.AnyType
import com.lollypop.runtime.instructions.invocables.Import.importKind
import com.lollypop.runtime.plastics.RuntimeClass
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import scala.annotation.tailrec

/**
 * Import Statement
 * @example {{{
 *   import 'java.util.Date'
 * }}}
 * @example {{{
 *   import JDate: 'java.util.Date'
 * }}}
 * @example {{{
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
        classNames.foldLeft[Scope](scope) { (agg, className) => importKind(className, target.alias)(agg) } -> value
      case className: String if className.nonEmpty => importKind(className, target.alias)(scope) -> value
      case x => dieIllegalType(x)
    }

    val (scope1, cost1, result1) = target.execute(scope0)
    val (scope2, result2) = recurse(scope1, result1)
    (scope2, cost1, result2)
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

  /**
   * Imports a kind; a class, object or native type
   * @param classQName the fully qualified class name (e.g. "java.util.Date")
   * @param alias      the optional alias for the kind being imported (e.g. "JDate")
   * @param scope      the implicit [[Scope scope]]
   * @return a new [[Scope scope]] populated with the imported kind
   * @example {{{
   *   import 'java.util.Date'
   * }}}
   * @example {{{
   *   import JDate: 'java.util.Date'
   * }}}
   * @example {{{
   *   import ['java.util.Date', 'java.sql.DateTime']
   * }}}
   */
  def importKind(classQName: String, alias: Option[String] = None)(implicit scope: Scope): Scope = {
    val _class = RuntimeClass.getClassByName(classQName)(scope)
    val localName = alias || _class.getSimpleName
    scope
      .withVariable(localName, `type` = AnyType(className = classQName), value = _class, isReadOnly = false)
      .withImports(Map(localName -> classQName))
  }

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Import] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      Some(Import(target = params.expressions("target")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    (ts is "import") && (ts isnt "import implicit")
  }
}