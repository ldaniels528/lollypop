package com.qwery.language.instructions

import com.qwery.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_DECLARATIVE}
import com.qwery.language.models.SourceCodeInstruction.RichSourceCodeInstruction
import com.qwery.language.models.{Instruction, Literal}
import com.qwery.language.{DirectiveParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.util.ResourceHelper.AutoClose
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileInputStream}

/**
 * Include directive
 */
object Include extends DirectiveParser {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "include",
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "include `file`" ,
    description = "incorporates the contents of an external file into current scope",
    example =
      """|include('./contrib/examples/src/main/qwery/Stocks.sql')
         |""".stripMargin
  ))

  override def parseDirective(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Instruction] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, "include %e:file")
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
