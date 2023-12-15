package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.{LifestyleExpressions, LifestyleExpressionsAny, LollypopUniverse}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVMSQL}
import org.scalatest.funspec.AnyFunSpec

class TransferToTest extends AnyFunSpec with VerificationTools {
  private val ctx = LollypopUniverse(isServerMode = true)
  private val compiler = LollypopCompiler(ctx)

  describe(classOf[TransferTo].getSimpleName) {

    it("should compile: 'Hello World' ===> stderr") {
      val model = compiler.compile(""""Hello World" ===> stderr""")
      assert(model == TransferTo("Hello World".v, "stderr".f))
    }

    it("should decompile: 'Hello World' ===> stderr") {
      val model = TransferTo("Hello World".v, "stderr".f)
      assert(model.toSQL == """"Hello World" ===> stderr""")
    }

    it("should write a String to STDERR") {
      """|"Hello World" ===> stderr
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdErr.asString() == "Hello World")
    }

    it("should write a String to STDOUT") {
      """|"Hello World" ===> stdout
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdOut.asString() == "Hello World")
    }

    it("should write a BLOB to STDOUT") {
      """|x = BLOB("Hello World")
         |x ===> stdout
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdOut.asString() == "Hello World")
    }

    it("should write a Character Array to STDERR") {
      """|('.' * 15) ===> stderr
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdErr.asString() == "...............")
    }

    it("should write a CLOB to STDERR") {
      """|x = CLOB("Hello World")
         |x ===> stderr
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdErr.asString() == "Hello World")
    }

    it("should write an Exception to STDERR") {
      """|def boom() := ???
         |try boom() catch e => e ===> stderr
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdErr.asString().take(75) == "com.lollypop.LollypopException: an implementation is missing on line 2 at 5")
    }

    it("should write a File to STDOUT") {
      """|import "java.io.File"
         |f = new File("app/core/src/main/resources/log4j.properties")
         |f ===> stdout
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdOut.asString().trim ==
        """|# Root logger option
           |log4j.rootLogger=INFO, stdout
           |
           |# Direct log messages to stdout
           |log4j.appender.stdout=org.apache.log4j.ConsoleAppender
           |log4j.appender.stdout.Target=System.out
           |log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
           |log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
           |""".stripMargin.trim)
    }

    it("should write an InputStream to STDOUT") {
      """|import "java.io.FileInputStream"
         |x = new FileInputStream("app/core/src/main/resources/log4j.properties")
         |x ===> stdout
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdOut.asString().trim ==
        """|# Root logger option
           |log4j.rootLogger=INFO, stdout
           |
           |# Direct log messages to stdout
           |log4j.appender.stdout=org.apache.log4j.ConsoleAppender
           |log4j.appender.stdout.Target=System.out
           |log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
           |log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
           |""".stripMargin.trim)
    }

    it("should write a SQLXML to STDERR") {
      """|x = SQLXML("Hello World")
         |x ===> stderr
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdErr.asString() == "Hello World")
    }

    it("should write a Table to STDOUT") {
      """|x =
         |  |------------------|
         |  | exchange | total |
         |  |------------------|
         |  | NASDAQ   |    24 |
         |  | AMEX     |     5 |
         |  | NYSE     |    28 |
         |  | OTCBB    |    32 |
         |  | OTHEROTC |     7 |
         |  |------------------|
         |x ===> stdout
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdOut.asString() ==
        """||------------------|
           || exchange | total |
           ||------------------|
           || NASDAQ   |    24 |
           || AMEX     |     5 |
           || NYSE     |    28 |
           || OTCBB    |    32 |
           || OTHEROTC |     7 |
           ||------------------|
           |""".stripMargin.trim)
    }

    it("should write an URL to STDOUT") {
      """|x = OS.getResource("/lootBox.txt")
         |assert(x isnt null, "Resource not found")
         |x ===> stdout
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdOut.asString().trim ==
        """|com.lollypop.runtime.instructions.queryables.LollypopComponentsTest$LootBox$
           |""".stripMargin.trim)
    }

  }

}
