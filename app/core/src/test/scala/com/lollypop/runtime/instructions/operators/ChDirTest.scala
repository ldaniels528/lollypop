package com.lollypop.runtime.instructions.operators

import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.repl.ChDir
import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL
import com.lollypop.runtime.Scope
import org.scalatest.funspec.AnyFunSpec

class ChDirTest extends AnyFunSpec {

  describe(classOf[ChDir].getSimpleName) {

    it("should compile: cd") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|cd
           |""".stripMargin)
      assert(model == ChDir(None))
    }

    it("should compile: cd '/some/fake/path'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|cd '/some/fake/path'
           |""".stripMargin)
      assert(model == ChDir(Some("/some/fake/path".v)))
    }

    it("should exercise the `chdir` lifecycle") {
      val (sb, _, vb) =
        """|cd '/some/fake/path'
           |""".stripMargin.executeSQL(createRootScope())
      assert(vb == ())

      val (sc, _, vc) =
        """|cd
           |""".stripMargin.executeSQL(sb)
      assert(vc == "/some/fake/path")

      val (sd, _, vd) =
        """|cd '.'
           |""".stripMargin.executeSQL(sc)
      assert(vd == "/some/fake/path")

      val (se, _, ve) =
        """|cd '..'
           |""".stripMargin.executeSQL(sd)
      assert(ve == "/some/fake")

      val (sy, _, vy) =
        """|cd '-'
           |""".stripMargin.executeSQL(se)
      assert(vy == "/some/fake/path")

      val (_, _, vz) =
        """|cd 'to/a/resource/'
           |cd
           |""".stripMargin.executeSQL(sy)
      assert(vz == "/some/fake/path/to/a/resource")
    }

  }

  private def createRootScope(): Scope = {
    s"""|lollypopComponents("${ChDir.getClass.getName}")
        |""".stripMargin.executeSQL(Scope())._1
  }

}
