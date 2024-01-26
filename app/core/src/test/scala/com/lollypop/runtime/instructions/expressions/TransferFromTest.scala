package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.{LifestyleExpressions, LifestyleExpressionsAny, LollypopUniverse}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVMSQL}
import org.scalatest.funspec.AnyFunSpec

class TransferFromTest extends AnyFunSpec with VerificationTools {
  private val ctx = LollypopUniverse(isServerMode = true)
  private val compiler = LollypopCompiler(ctx)

  describe(classOf[TransferFrom].getSimpleName) {

    it("should compile: stderr <=== 'Hello World'") {
      val model = compiler.compile("""stderr <=== "Hello World"""")
      assert(model == TransferFrom("stderr".f, "Hello World".v))
    }

    it("should decompile: stderr <=== 'Hello World'") {
      val model = TransferFrom("stderr".f, "Hello World".v)
      assert(model.toSQL == """stderr <=== "Hello World"""")
    }

    it("should write a String to STDERR") {
      """|stderr <=== "Hello World"
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdErr.asString() == "Hello World")
    }

    it("should write a String to STDOUT") {
      """|stdout <=== "Hello World"
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdOut.asString() == "Hello World")
    }

    it("should write a BLOB to STDOUT") {
      """|x = BLOB("Hello World")
         |stdout <=== x
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdOut.asString() == "Hello World")
    }

    it("should write a Character Array to STDERR") {
      """|stderr <=== ['A' to 'G']
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdErr.asString() == "ABCDEFG")
    }

    it("should write a CLOB to STDERR") {
      """|x = CLOB("Hello World")
         |stderr <=== x
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdErr.asString() == "Hello World")
    }

    it("should write an Exception to STDERR") {
      """|def boom() := ???
         |try boom() catch e => stderr <=== e
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdErr.asString().take(75) == "com.lollypop.LollypopException: an implementation is missing on line 2 at 5")
    }

    it("should write a File to STDOUT") {
      """|import "java.io.File"
         |f = new File("app/core/src/main/resources/log4j.properties")
         |stdout <=== f
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
         |stdout <=== x
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
         |stderr <=== x
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
         |stdout <=== x
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
         |stdout <=== x
         |""".stripMargin.executeSQL(ctx.createRootScope())
      assert(ctx.system.stdOut.asString().trim ==
        """|com.lollypop.runtime.instructions.queryables.LollypopComponentsTest$LootBox$
           |""".stripMargin.trim)
    }

  }

}