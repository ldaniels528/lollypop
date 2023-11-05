package com.lollypop.examples

import com.lollypop.runtime.LollypopCompiler
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.RuntimeFiles.implicits.string2File
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.io.File

class ExamplesTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val examplesDirectory = ("app": File) / "examples" / "src" / "main" / "lollypop"
  private val scripts =
    """|shocktrade/shocktrade.sql
       |BlackJack.sql
       |GenerateVinMapping.sql
       |MacroDemo.sql
       |SwingDemo.sql
       |BreakOutDemo.sql
       |IngestDemo.sql
       |Stocks.sql
       |""".stripMargin
      .replace("\n", " ").split(" ").map(_.trim).filter(_.nonEmpty)

  describe("Examples") {

    it("all examples should compile") {
      val compiler = LollypopCompiler()
      scripts foreach { script =>
        val file = examplesDirectory / script
        logger.info(s"Compiling '${file.getPath}'...")
        compiler.compile(file)
      }
    }

  }

}
