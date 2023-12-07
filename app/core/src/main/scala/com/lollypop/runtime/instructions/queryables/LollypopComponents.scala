package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Expression, Literal}
import com.lollypop.runtime.LollypopVM.rootScope
import com.lollypop.runtime.datatypes.StringType
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.runtime.instructions.queryables.LollypopComponents.{installComponents, keyword, toTable}
import com.lollypop.runtime.plastics.RuntimeClass
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Installs a collection of components from a source.
 * @example {{{
 *  lollypopComponents('com.lollypop.repl.gnu.ChDir$')
 *  cd './lollypop_db'
 *  cd
 * }}}
 * @example {{{
 *  lollypopComponents("com.lollypop.repl.gnu.Ls$")
 *  ls './app/examples/'
 * }}}
 * @example {{{
 *   lollypopComponents(new `java.io.File`("lootBox.txt"))
 * }}}
 * @example {{{
 *   lollypopComponents("/lootBox.txt".toResourceURL())
 * }}}
 */
case class LollypopComponents(src: Expression) extends RuntimeQueryable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    (scope, IOCost.empty, toTable(installComponents(src)))
  }

  override def toString: String = s"$keyword(${src.toString})"

  override def toSQL = s"$keyword ${src.wrapSQL}"

}

/**
 * Lollypop Components Companion
 */
object LollypopComponents extends QueryableParser {
  private val keyword = "lollypopComponents"
  private val templateCard = s"$keyword %e:source"

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[LollypopComponents] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      params.expressions.get("source").map {
        case v@Literal(_: String) =>
          installComponents(v)(rootScope)
          LollypopComponents(v)
        case v => LollypopComponents(v)
      }
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    ts is keyword
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "installs a collection of components from a source.",
    example =
      """|lollypopComponents("com.lollypop.repl.gnu.Ls$")
         |ls "./app/examples/"
         |""".stripMargin,
    isExperimental = true
  ))

  private def installComponents(sourceExpr: Expression)(implicit scope: Scope): List[ComponentInfo] = {
    val (sa, ca, stream) = sourceExpr.pullInputStream
    val classLoader = sa.getUniverse.classLoader
    Source.fromInputStream(stream).use(_.getLines().map(_.trim).filter(_.nonEmpty).toList.map {
      case s if s.endsWith("$") =>
        installComponent(s, () => RuntimeClass.getObjectByName(s.dropRight(1))(classLoader))(sa)
      case s =>
        installComponent(s, () =>
          Class.forName(s).getConstructors.find(_.getParameterCount == 0)
            .map(_.newInstance()) || sourceExpr.die("No zero-parameter constructor found"))(sa)
    })
  }

  private def installComponent(className: String, f: () => Any)(implicit scope: Scope): ComponentInfo = {
    val (name, kind, status) = Try(f() match {
      case lp: LanguageParser =>
        val name = lp.help.headOption.map(_.name) || lp.getClass.getSimpleName
        scope.getUniverse.withLanguageParsers(lp)
        Some(name) -> "LanguageParser"
      case _ => None -> "Unknown"
    }) match {
      case Success((Some(name), kind)) => (name, kind, "Installed")
      case Success((None, kind)) => (null, kind, "Not Installed")
      case Failure(e) => (null, null, "FAILED: " + e.getMessage)
    }
    ComponentInfo(name, kind, status, className)
  }

  private def toTable(components: Iterable[ComponentInfo]): RowCollection = {
    val out = createQueryResultTable(Seq(
      TableColumn(name = "name", `type` = StringType),
      TableColumn(name = "kind", `type` = StringType),
      TableColumn(name = "status", `type` = StringType),
      TableColumn(name = "className", `type` = StringType)
    ))
    components.foreach { ci =>
      out.insert(Map("name" -> ci.name, "kind" -> ci.kind, "status" -> ci.status, "className" -> ci.className).toRow(out))
    }
    out
  }

  case class ComponentInfo(name: String, kind: String, status: String, className: String)

}