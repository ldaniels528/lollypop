package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.Expression.implicits.RichAliasable
import com.lollypop.language.models.{Condition, Queryable}
import com.lollypop.runtime.instructions.conditions.RuntimeCondition.RichConditionAtRuntime
import com.lollypop.runtime.instructions.expressions._
import com.lollypop.runtime.instructions.queryables.AssumeQueryable.EnrichedAssumeQueryable
import com.lollypop.util.OptionHelper.OptionEnrichment

import scala.language.{existentials, postfixOps}

/**
  * Base class for all Join expressions
  * @author lawrence.daniels@gmail.com
  */
sealed trait Join extends Queryable {

  /**
    * @return the join [[Queryable source]]
    */
  def source: Queryable

  /**
   * @return the join [[Condition condition]]
   */
  def condition: Condition

}

object Join extends QueryableChainParser {
  private val template = "%q:join_src on %c:join_cond"

  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    queryableChains.collectFirst { case (keywords, parser) if ts nextIf keywords =>
      parser(ts, host)
    } || ts.dieExpectedQueryable()
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "inner join",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Computes the inner join of two queries",
    example =
      """|namespace 'temp.examples'
         |drop if exists stockQuotes_A
         |create table stockQuotes_A (symbol: String(32), exchange: String(32), lastSale: Double)
         |insert into stockQuotes_A (symbol, exchange, lastSale)
         |values ('GREED', 'NASDAQ', 2345.78), ('BFG', 'NYSE', 113.56),
         |       ('ABC', 'AMEX', 11.46), ('ACME', 'NYSE', 56.78)
         |create index stockQuotes_A#symbol
         |
         |drop if exists companies_A
         |create table companies_A (symbol: String(32), name: String(32))
         |insert into companies_A (symbol, name)
         |values ('ABC', 'ABC & Co'), ('BFG', 'BFG Corp.'),
         |       ('GREED', 'GreedIsGood.com'), ('ACME', 'ACME Inc.')
         |create index companies_A#symbol
         |
         |select B.name, A.symbol, A.exchange, A.lastSale
         |from stockQuotes_A as A
         |inner join companies_A as B on A.symbol is B.symbol
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = isQueryableChain(ts)

  private def isQueryableChain(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    queryableChains.exists { case (keywords, _) => ts is keywords }
  }

  private def queryableChains(implicit compiler: SQLCompiler): Seq[(String, (TokenStream, Queryable) => Queryable)] = Seq(
    "cross join" -> { (ts, q) => chainQueryableJoin(ts, q, CrossJoin) },
    "full join" -> { (ts, q) => chainQueryableJoin(ts, q, FullOuterJoin) },
    "inner join" -> { (ts, q) => chainQueryableJoin(ts, q, InnerJoin) },
    "left join" -> { (ts, q) => chainQueryableJoin(ts, q, LeftOuterJoin) },
    "right join" -> { (ts, q) => chainQueryableJoin(ts, q, RightOuterJoin) }
  )

  private def chainQueryableJoin[A <: Join](ts: TokenStream, q: Queryable, f: (Queryable, Condition) => A)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(ts, template)
    val join_src = params.instructions("join_src").asQueryable
    val join_cond = params.conditions.get("join_cond").map(_.transform {
      case Infix(t, f) => JoinFieldRef(t.getNameOrDie, f.getNameOrDie)
      case expr => expr
    }) || ts.dieExpectedCondition()
    val join = f(join_src, join_cond)
    q match {
      case s: Select => s.copy(joins = s.joins.toList ::: List(join))
      case _ => Select(from = Option(q), joins = List(join))
    }
  }

}

case class CrossJoin(source: Queryable, condition: Condition) extends Join {
  override def toSQL: String = s"cross join ${source.toSQL} on ${condition.toSQL}"
}

case class FullOuterJoin(source: Queryable, condition: Condition) extends Join {
  override def toSQL: String = s"full join ${source.toSQL} on ${condition.toSQL}"
}

case class InnerJoin(source: Queryable, condition: Condition) extends Join {
  override def toSQL: String = s"inner join ${source.toSQL} on ${condition.toSQL}"
}

case class LeftOuterJoin(source: Queryable, condition: Condition) extends Join {
  override def toSQL: String = s"left join ${source.toSQL} on ${condition.toSQL}"
}

case class RightOuterJoin(source: Queryable, condition: Condition) extends Join {
  override def toSQL: String = s"right join ${source.toSQL} on ${condition.toSQL}"
}
