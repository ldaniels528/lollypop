package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{HelpDoc, QueryableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.Cat.keyword
import com.lollypop.runtime.datatypes.StringType
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices.{LazyRowCollection, RowCollection, TableColumn}
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost
import lollypop.lang.OS.ReaderIterator

/**
 * Cat - Retrieves the contents of a file
 * @param expression the [[Expression expression]]
 * @example {{{
 *   cat 'README.md' limit 5
 * }}}
 */
case class Cat(expression: Expression) extends RuntimeQueryable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    expression.pullFile map { file =>
      val out = createQueryResultTable(columns = Seq(TableColumn(name = keyword, `type` = StringType)))
      LazyRowCollection(out, new ReaderIterator(file).map(line => Map(keyword -> line).toRow(out)))
    }
  }

  override def toSQL: String = Seq(keyword, expression.toSQL).mkString(" ")

}

object Cat extends QueryableParser {
  private val keyword = "cat"
  private val templateCard = s"$keyword %e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Retrieves the contents of a file.",
    example =
      """|cat 'app/examples/src/main/resources/log4j.properties'
         |""".stripMargin
  ))

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Cat] = {
    if (understands(ts)) {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Cat(p.expressions("path")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}