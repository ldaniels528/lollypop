package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_SYSTEMS, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.RuntimeClass.{downloadDependencies, loadJarFiles}
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

/**
 * Require Statement
 * @example require "log4j:log4j:1.2.17"
 * @example require ["org.apache.spark:spark-core_2.13:3.3.0", "org.apache.spark:spark-sql_2.13:3.3.0"]
 */
case class Require(target: Expression) extends RuntimeInvokable {

  override def invoke()(implicit scope: Scope): (Scope, IOCost, Any) = {
    // download the dependencies as jars
    val (s, c, r) = QweryVM.execute(scope, target)
    val files_? = Option(r) map {
      case array: Array[_] =>
        val dependencies = array.map { case s: String => s; case x => dieIllegalType(x) }
        downloadDependencies(dependencies: _*)
      case dependency: String if dependency.nonEmpty => downloadDependencies(dependency)
      case x => dieIllegalType(x)
    }

    // load the jars
    files_?.foreach(loadJarFiles)
    (scope, c, null)
  }

  override def toSQL: String = s"require ${target.toSQL}"
}

/**
 * Require Companion
 */
object Require extends InvokableParser {
  val templateCard = "require %e:target"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "require",
    category = CATEGORY_SYSTEMS,
    paradigm = PARADIGM_OBJECT_ORIENTED,
    syntax = templateCard,
    description = "Downloads a JVM dependency (jar) from a repository",
    example = "require ['org.apache.spark:spark-core_2.13:3.3.0']"
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Require = {
    val params = SQLTemplateParams(ts, templateCard)
    Require(target = params.expressions("target"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "require"
}