package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_IMPERATIVE}
import com.lollypop.language._
import com.lollypop.language.models.Expression
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Sets the database and schema
 * @param expression the database/schema path (e.g. "securities.stocks")
 */
case class Namespace(expression: Expression) extends RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (s, c, r) = expression.execute(scope)
    val s1 = r match {
      case null => scope
      case path: String =>
        path.split("[.]") match {
          case Array(database, schema) => scope.withDatabase(database).withSchema(schema)
          case Array(database) => s.withDatabase(database)
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
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = template,
    description = "Sets the active database",
    example =
      """|namespace 'stocks_demo'
         |__namespace__
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Namespace] = {
    if (understands(ts)) {
      Some(Namespace(expression = SQLTemplateParams(ts, template).expressions("path")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = Set("use", "namespace").exists(ts is _)

}
