package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.expressions.ZipWith.keyword
import com.lollypop.runtime.plastics.Tuples.tupleToSeq
import com.lollypop.util.OptionHelper.OptionEnrichment

case class ZipWith(exprA: Expression, exprB: Expression) extends RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Any = {
    val arrayA = exprA.asArray || exprA.dieIllegalType()
    val arrayB = exprB.asArray || exprB.dieIllegalType()
    (arrayA zip arrayB).flatMap(tupleToSeq).map(_.toArray)
  }

  override def toSQL: String = Seq(exprA.toSQL, keyword, exprB.toSQL).mkString(" ")
}

object ZipWith extends ExpressionChainParser {
  private val keyword = "<|>"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = s"array0 $keyword array1",
    description = "Horizontally combines two arrays.",
    example =
      """|['a', 'b', 'c'] <|> [1, 2, 3]
         |""".stripMargin
  ))

  override def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[ZipWith] = {
    if (ts nextIf keyword) Option(ZipWith(host, compiler.nextExpression(ts) || host.dieExpectedExpression())) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}