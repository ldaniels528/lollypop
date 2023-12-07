package com.lollypop.language.instructions

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.SourceCodeInstruction.RichSourceCodeInstruction
import com.lollypop.language.models.{Instruction, Literal}
import com.lollypop.runtime._
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileInputStream}

/**
 * Include directive - incorporates the contents of an external file into current scope.
 * @example {{{
 *  include('./app/examples/src/main/lollypop/Stocks.sql')
 * }}}
 */
object Include extends DirectiveParser {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val templateCard = "include %e:file"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "include",
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "incorporates the contents of an external file into current scope",
    example =
      """|include('./app/examples/src/main/lollypop/Stocks.sql')
         |""".stripMargin
  ))

  override def parseDirective(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Instruction] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      val code = params.expressions("file") match {
        case Literal(path: String) =>
          logger.info(s"Including '$path'...")
          val sourceFile = new File(path)
          new FileInputStream(sourceFile).use(compiler.compile).updateFile(sourceFile)
        case other => ts.dieIllegalType(other)
      }
      Option(code)
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "include"

}
