package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_BRANCHING_OPS, PARADIGM_IMPERATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.devices.QMap
import com.qwery.runtime.{QweryVM, Scope}

case class Contains(items: Expression, item: Expression) extends RuntimeCondition {

  override def isTrue(implicit scope: Scope): Boolean = {
    (for {
      container <- Option(QweryVM.execute(scope, items)._3)
      elem <- Option(QweryVM.execute(scope, item)._3)
    } yield {
      container match {
        case a: Array[Any] => a.contains(elem)
        case m: QMap[String, Any] => m.contains(elem.toString)
        case s: String => s.contains(elem.toString)
        case _ => false
      }
    }).contains(true)
  }

  override def toSQL: String = s"${items.toSQL} contains ${item.toSQL}"

}

object Contains extends ExpressionToConditionPostParser {
  private val __name = "contains"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Contains] = {
    if (ts.nextIf(__name)) compiler.nextExpression(ts).map(Contains(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __name,
    category = CATEGORY_BRANCHING_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = s"`value` ${__name} `expression`",
    description = "determines whether the `value` contains the `expression`",
    example =
      """|string = "Hello World"
         |string contains "World"
         |""".stripMargin
  ), HelpDoc(
    name = __name,
    category = CATEGORY_BRANCHING_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = s"`value` ${__name} `expression`",
    description = "determines whether the `value` contains the `expression`",
    example =
      """|dict = {"name":"Tom", "DOB":"2003-09-28T00:00:00.000Z"}
         |dict contains "name"
         |""".stripMargin
  ), HelpDoc(
    name = __name,
    category = CATEGORY_BRANCHING_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = s"`value` ${__name} `expression`",
    description = "determines whether the `value` contains the `expression`",
    example =
      """|array = [{"name":"Jerry"}, {"name":"Tom"}, {"name":"Sheila"}]
         |array contains {"name":"Tom"}
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}