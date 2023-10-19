package com.qwery.runtime.instructions.expressions

import com.qwery.language.HelpDoc.{CATEGORY_SESSION, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Plastic.tupleToSeq
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.instructions.expressions.ZipWith.keyword
import com.qwery.util.OptionHelper.OptionEnrichment

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
    category = CATEGORY_SESSION,
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