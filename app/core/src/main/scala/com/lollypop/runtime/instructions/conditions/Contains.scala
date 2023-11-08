package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.QMap
import lollypop.io.IOCost

case class Contains(items: Expression, item: Expression) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (sa, ca, ra) = items.execute(scope)
    val (sb, cb, rb) = item.execute(sa)
    val result = (for {
      container <- Option(ra)
      elem <- Option(rb)
    } yield {
      container match {
        case a: Array[Any] => a.contains(elem)
        case m: QMap[String, Any] => m.contains(elem.toString)
        case s: String => s.contains(elem.toString)
        case _ => false
      }
    }).contains(true)
    (sb, ca ++ cb, result)
  }

  override def toSQL: String = s"${items.toSQL} contains ${item.toSQL}"

}

object Contains extends ExpressionToConditionPostParser {
  private val keyword = "contains"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Contains] = {
    if (ts.nextIf(keyword)) compiler.nextExpression(ts).map(Contains(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`value` $keyword `expression`",
    description = "determines whether the `value` contains the `expression`",
    example =
      """|string = "Hello World"
         |string contains "World"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`value` $keyword `expression`",
    description = "determines whether the `value` contains the `expression`",
    example =
      """|dict = {"name":"Tom", "DOB":"2003-09-28T00:00:00.000Z"}
         |dict contains "name"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`value` $keyword `expression`",
    description = "determines whether the `value` contains the `expression`",
    example =
      """|array = [{"name":"Jerry"}, {"name":"Tom"}, {"name":"Sheila"}]
         |array contains {"name":"Tom"}
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}