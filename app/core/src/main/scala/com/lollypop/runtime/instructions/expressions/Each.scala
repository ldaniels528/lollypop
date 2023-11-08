package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Atom, Expression, Instruction, Queryable}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.datatypes.StringType
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices.{QMap, Row, RowCollection, TableColumn}
import com.lollypop.runtime.instructions.expressions.Each.EnrichedBlockDevice
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.queryables.TableRendering
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import scala.collection.concurrent.TrieMap

/**
 * each dataframe iteration
 * @param variable  the given [[Atom variable]]
 * @param queryable the given [[Queryable rows]]
 * @param code      the [[Instruction statements]] to execute
 * @param isYield   indicates whether a table result is to be generated and returned
 * @param limit     the optional [[Expression maximum number]] of rows to retrieve from the source
 * @example
 * {{{
 * each item in reverse (select symbol, lastSale from Securities where naics = '12345') limit 20 {
 *   out.println '{{item.symbol}} is {{item.lastSale}}/share'
 * }
 * }}}
 * @example
 * {{{
 * set pennyStocks =
 *    each item in (select symbol, lastSale from Securities where lastSale < 1.0) yield {
 *      select item.symbol, item.lastSale, item.exchange
 *    }
 * }}}
 */
case class Each(variable: Atom,
                queryable: Instruction,
                code: Instruction,
                isYield: Boolean = false,
                limit: Option[Expression] = None) extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {

    def fixTypes(columns: TableColumn): TableColumn = columns match {
      case c if c.`type`.name == StringType.name =>
        c.copy(`type` = if (c.`type`.maxSizeInBytes < StringType.maxSizeInBytes) StringType else c.`type`)
      case c => c
    }

    def processRow(row: Row): Option[Any] = {
      val newScope = scope
        .withVariable(variable.name, value = Some(row))
        .withCurrentRow(Some(row))
      Option(code.execute(newScope)._3)
    }

    def processSource(in: RowCollection): Option[RowCollection] = {
      val factory = TrieMap[Unit, RowCollection]()
      val _limit = limit.flatMap(_.asInt32)
      val processor = if (isYield) processYield(factory) else processRow _
      in.foreachWithLimit(_limit, processor)
      factory.get(())
    }

    def processYield(factory: TrieMap[Unit, RowCollection]): Row => Option[Any] = { row =>
      def write(src: RowCollection): Option[Any] = {
        val out = factory.getOrElseUpdate((), createQueryResultTable(src.columns.map(fixTypes)))
        src.foreach(row => out.insert(row))
        None
      }

      processRow(row) map {
        case rc: RowCollection => write(rc)
        case tr: TableRendering => write(tr.toTable)
        case row: Row => write(row.toRowCollection)
        case m: QMap[_, _] => write(m.toKeyValueCollection)
        case z => code.dieIllegalType(z)
      }
    }

    // iterate the rows (forward or reverse)
    val (s, c, rc) = queryable.search(scope)
    (s, c, (for {item <- processSource(rc)} yield item).orNull)
  }

  override def toSQL: String = {
    ("each" :: variable.toSQL :: "in" :: queryable.wrapSQL(true) ::
      limit.toList.flatMap(e => List("limit", e.toSQL)) :::
      (if (isYield) List("yield") else Nil) ::: code.toSQL :: Nil).mkString(" ")
  }

}

object Each extends ExpressionParser {
  val templateCard: String = "each %a:variable in %q:rows ?limit +?%e:limit ?yield %N:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "each",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Iterates over a dataframe applying a function to each entry",
    example =
      """|stocks =
         ||---------------------------------------------------------|
         || symbol | exchange | lastSale | lastSaleTime             |
         ||---------------------------------------------------------|
         || LORI   | NYSE     |  89.6033 | 2023-07-29T04:09:04.524Z |
         || AVSR   | AMEX     | 477.5694 | 2023-07-29T04:09:04.529Z |
         || KXYP   | OTCBB    | 475.6416 | 2023-07-29T04:09:04.531Z |
         || JYVV   | NYSE     | 197.1071 | 2023-07-29T04:09:04.532Z |
         || EVDX   | OTCBB    |  77.1829 | 2023-07-29T04:09:04.533Z |
         ||---------------------------------------------------------|
         |var messages = []
         |each item in (select symbol, lastSale from @stocks where lastSale < 100)
         |  messages = messages.push('{{item.symbol}} is {{item.lastSale}}/share')
         |messages
         |""".stripMargin
  ), HelpDoc(
    name = "each",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Iterates a dataframe in reverse order applying a function to each entry",
    example =
      """|stocks =
         ||----------------------------------------------------------|
         || exchange  | symbol | lastSale | lastSaleTime             |
         ||----------------------------------------------------------|
         || OTHER_OTC | WAEQK  |   0.6713 | 2023-10-14T18:40:32.998Z |
         || NASDAQ    | RQNMU  |  19.6168 | 2023-10-14T18:40:32.335Z |
         || NASDAQ    | WP     |  45.7338 | 2023-10-14T18:40:24.264Z |
         || OTHER_OTC | NNFO   |    0.151 | 2023-10-14T18:39:51.236Z |
         || NASDAQ    | LVEBB  |   8.4378 | 2023-10-14T18:39:58.491Z |
         || NASDAQ    | SWTD   |  22.5552 | 2023-10-14T18:39:50.783Z |
         || OTHER_OTC | CZYBQ  |   0.8543 | 2023-10-14T18:40:40.513Z |
         || NASDAQ    | CQ     | 174.0586 | 2023-10-14T18:39:55.858Z |
         || NYSE      | QOVGA  |   1.9199 | 2023-10-14T18:40:14.590Z |
         || NYSE      | ZJWL   |  17.3107 | 2023-10-14T18:40:13.205Z |
         ||----------------------------------------------------------|
         |each item in (stocks.reverse()) yield item
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Each] = {
    val params = SQLTemplateParams(ts, templateCard)
    Some(Each(variable = params.atoms("variable"), queryable = params.instructions("rows"),
      isYield = params.keywords.contains("yield"),
      limit = params.expressions.get("limit"),
      code = params.instructions("code")))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "each"

  final implicit class EnrichedBlockDevice(val device: RowCollection) extends AnyVal {

    def foreachWithLimit[U](limit: Option[Int] = None, callback: Row => U): Unit = {
      for {
        rowID <- 0L until (limit.map(_.toLong) || device.getLength)
        row <- device.get(rowID)
      } callback(row)
    }

  }

}