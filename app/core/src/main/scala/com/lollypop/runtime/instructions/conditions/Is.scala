package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.{Condition, Expression}
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.conditions.Is.keyword
import lollypop.io.IOCost

/**
 * SQL: `a` is equal to `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class Is(a: Expression, b: Expression) extends RuntimeInequality {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (sa, ca, ra) = a.execute(scope)
    val (sb, cb, rb) = b.execute(sa)
    (sb, ca ++ cb, ra == rb)
  }

  override def operator: String = keyword

}

object Is extends ExpressionToConditionPostParser {
  private val keyword = "is"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts.nextIf(keyword)) {
      if (ts.nextIf("not null")) Option(IsNotNull(host))
      else if (ts.nextIf("not")) compiler.nextExpression(ts).map(Isnt(host, _))
      else if (ts.nextIf("null")) Option(IsNull(host))
      else compiler.nextExpression(ts).map(Is(host, _))
    } else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "is",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "`value` is `expression`",
    description = "returns true if the `value` is exactly the `expression`; otherwise false",
    example =
      """|x = 200
         |x is 200
         |""".stripMargin
  ), HelpDoc(
    name = "is",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "`value` is `expression`",
    description = "returns true if the `value` is exactly the `expression`; otherwise false",
    example =
      """|x = 201
         |x is 200
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
