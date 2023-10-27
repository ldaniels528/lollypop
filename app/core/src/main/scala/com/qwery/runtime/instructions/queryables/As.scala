package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_DECLARATIVE}
import com.qwery.language.Token.AtomToken
import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}

object As extends ExpressionChainParser {
  private val templateCard = "as %i:code"

  override def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts.nextIf("as")) {
      val identifier = ts.peek match {
        case Some(_: AtomToken) => ts.next().valueAsString
        case None => ts.die("Unexpected end of statement")
      }
      Option(host.as(identifier))
    } else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "as",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Applies an alias to an expression or query",
    example =
      """|stocks =
         ||---------------------------------------------------------|
         || exchange | symbol | lastSale | lastSaleTime             |
         ||---------------------------------------------------------|
         || NASDAQ   | RY     |  68.6234 | 2023-09-28T22:25:55.559Z |
         || OTCBB    | OUSVN  |   0.7195 | 2023-09-28T22:25:59.404Z |
         || NYSE     | FTR    |  40.7124 | 2023-09-28T22:26:21.811Z |
         || OTCBB    | TWVD   |   0.0401 | 2023-09-28T22:26:10.017Z |
         || OTCBB    | GVHMN  |   0.9648 | 2023-09-28T22:25:57.608Z |
         || NASDAQ   | DS     |  155.021 | 2023-09-28T22:25:59.213Z |
         ||---------------------------------------------------------|
         |select count(*) as total,
         |       avg(lastSale) as avgLastSale,
         |       max(lastSale) as maxLastSale,
         |       min(lastSale) as minLastSale,
         |       sum(lastSale) as sumLastSale
         |from @@stocks
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "as"

}