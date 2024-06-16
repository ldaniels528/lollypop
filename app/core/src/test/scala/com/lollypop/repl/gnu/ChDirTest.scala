package com.lollypop.repl.gnu

import com.lollypop.language.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.repl.REPLFunSpec
import com.lollypop.repl.symbols.{Dot, Tilde}
import com.lollypop.runtime._
import com.lollypop.runtime.instructions.operators.Div

import java.io.File
import scala.util.Properties

class ChDirTest extends REPLFunSpec {

  describe(classOf[ChDir].getSimpleName) {

    it("should compile: cd") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|cd
           |""".stripMargin)
      assert(model == ChDir(None))
    }

    it("should compile: cd ~") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|cd ~
           |""".stripMargin)
      assert(model == ChDir(Some(Tilde())))
    }

    it("should compile: cd Downloads/images") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|cd Downloads/images
           |""".stripMargin)
      assert(model == ChDir(Some(Div("Downloads".f, "images".f))))
    }

    it("should compile: cd ~/Downloads") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|cd ~/Downloads
           |""".stripMargin)
      assert(model == ChDir(Some(Div(Tilde(), "Downloads".f))))
    }

    it("should compile: cd '/some/fake/path'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|cd '/some/fake/path'
           |""".stripMargin)
      assert(model == ChDir(Some("/some/fake/path".v)))
    }

    it("should decompile: cd") {
      val model = ChDir(None)
      assert(model.toSQL == "cd")
    }

    it("should decompile: cd ~") {
      val model = ChDir(Some(Tilde()))
      assert(model.toSQL == "cd ~")
    }

    it("should decompile: cd ./Documents") {
      val model = ChDir(Some(Div(Dot(), "Documents".f)))
      assert(model.toSQL == "cd . / Documents")
    }

    it("should decompile: cd ~/Downloads") {
      val model = ChDir(Some(Div(Tilde(), "Downloads".f)))
      assert(model.toSQL == "cd ~ / Downloads")
    }

    it("should decompile: cd '/some/fake/path'") {
      val model = ChDir(Some("/some/fake/path".v))
      assert(model.toSQL == """cd "/some/fake/path"""")
    }

    it("should execute: cd") {
      val (_, _, va) =
        """|cd
           |pwd
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == Properties.userHome)
    }

    it("should execute: cd ~/Downloads") {
      val (_, _, va) =
        """|cd ~/Downloads
           |pwd
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == Properties.userHome + File.separator + "Downloads")
    }

    it("should execute: cd '~/Downloads/'") {
      val (_, _, va) =
        """|cd '~/Downloads/'
           |pwd
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == Properties.userHome + File.separator + "Downloads")
    }

    it("should exercise the change directory lifecycle (strings)") {
      val (sb, _, vb) = "cd '/some/fake/path'".executeSQL(createRootScope())
      assert(vb == ())

      val (sc, _, vc) = "pwd".stripMargin.executeSQL(sb)
      assert(vc == "/some/fake/path")

      val (sd, _, vd) =
        """|cd '.'
           |pwd
           |""".stripMargin.executeSQL(sc)
      assert(vd == "/some/fake/path")

      val (se, _, ve) =
        """|cd '..'
           |pwd
           |""".stripMargin.executeSQL(sd)
      assert(ve == "/some/fake")

      val (sy, _, vy) =
        """|cd '_'
           |pwd
           |""".stripMargin.executeSQL(se)
      assert(vy == "/some/fake/path")

      val (_, _, vz) =
        """|cd 'to/a/resource/'
           |pwd
           |""".stripMargin.executeSQL(sy)
      assert(vz == "/some/fake/path/to/a/resource")
    }

    it("should exercise the change directory lifecycle (atoms)") {
      val (sb, _, vb) = "cd ^/some/fake/path".executeSQL(createRootScope())
      assert(vb == ())

      val (sc, _, vc) = "pwd".executeSQL(sb)
      assert(vc == "/some/fake/path")

      val (sd, _, vd) =
        """|cd .
           |pwd
           |""".stripMargin.executeSQL(sc)
      assert(vd == "/some/fake/path")

      val (sx, _, vx) =
        """|cd ..
           |pwd
           |""".stripMargin.executeSQL(sd)
      assert(vx == "/some/fake")

      val (sy, _, vy) =
        """|cd _
           |pwd
           |""".stripMargin.executeSQL(sx)
      assert(vy == "/some/fake/path")

      val (_, _, vz) =
        """|cd 'to'/a/resource
           |pwd
           |""".stripMargin.executeSQL(sy)
      assert(vz == "/some/fake/path/to/a/resource")
    }

  }

}
