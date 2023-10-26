package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_SESSION, PARADIGM_IMPERATIVE}
import com.qwery.language._
import com.qwery.language.models.Expression
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

/**
 * Sets the database and schema
 * @param expression the database/schema path (e.g. "securities.stocks")
 */
case class Namespace(expression: Expression) extends RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (s, c, r) = QweryVM.execute(scope, expression)
    val s1 = r match {
      case null => scope
      case path: String =>
        path.split("[.]") match {
          case Array(database, schema) => scope.withDatabase(database).withSchema(schema)
          case Array(database) => scope.withDatabase(database)
          case _ => expression.die("Incorrect arguments: namespace 'myDatabase[.mySchema]'")
        }
      case value => dieUnsupportedType(value)
    }
    (s1, c, null)
  }

  override def toSQL: String = s"use ${expression.toSQL}"
}

object Namespace extends InvokableParser {
  val template: String = "%C(name|namespace|use) %e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "namespace",
    category = CATEGORY_SESSION,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = template,
    description = "Sets the active database",
    example =
      """|namespace 'stocks_demo'
         |__namespace__
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Namespace = {
    Namespace(expression = SQLTemplateParams(ts, template).expressions("path"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = Set("use", "namespace").exists(ts is _)

}
