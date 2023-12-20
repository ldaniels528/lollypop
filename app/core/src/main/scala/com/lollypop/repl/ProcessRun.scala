package com.lollypop.repl

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.Token.ProcessInvocationToken
import com.lollypop.language._
import com.lollypop.repl.ProcessRun.invoke
import com.lollypop.runtime.datatypes.{Int32Type, StringType}
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import scala.sys.process.Process

/**
 * Represents a process invocation
 * @param sourceCode the native code/instructions (e.g. 'iostat 1 5')
 * @example {{{
 *   (% iostat 1 5 %)
 * }}}
 * @example {{{
 *   (% ps aux %) limit 5
 * }}}
 */
case class ProcessRun(sourceCode: String) extends RuntimeQueryable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    invoke(sourceCode.replaceVariables())
  }

  override def toSQL: String = s"(%$sourceCode%)"
}

object ProcessRun extends QueryableParser {
  private val templateCard = "(% %e:command %)"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "(%",
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Executes an application from the host operating system",
    example =
      """|(% iostat 1 3 %)
         |""".stripMargin,
    isExperimental = true
  ))

  def invoke(code: String)(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val columns = Seq("lineNumber" -> Int32Type, "output" -> StringType)
      .map { case (name, kind) => TableColumn(name, `type` = kind) }
    val columnNames = columns.map(_.name)
    val lines = Process(code).lazyLines.flatMap(_.split("[\n]")).zipWithIndex
    implicit val rc: RowCollection = createQueryResultTable(columns)
    val cost = lines.foldLeft(IOCost.empty) { case (agg, (line, n)) =>
      agg ++ rc.insert(Map(columnNames zip Seq(n + 1, line): _*).toRow)
    }
    (scope, cost, rc)
  }

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ProcessRun] = {
    if (understands(ts)) {
      ts.next() match {
        case ProcessInvocationToken(_, code, _, _) => Some(ProcessRun(code))
        case x => ts.dieIllegalType(x)
      }
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    ts.peek match {
      case Some(t: ProcessInvocationToken) => t.id == "%"
      case _ => false
    }
  }

}