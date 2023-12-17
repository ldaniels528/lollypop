package com.lollypop.runtime.instructions.jvm

import com.lollypop.language._
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class SuperClassesOfTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[SuperClassesOf].getSimpleName) {

    it("should compile: superClassesOf(classOf('java.util.ArrayList'))") {
      val model = compiler.compile("superClassesOf(classOf('java.util.ArrayList'))")
      assert(model == SuperClassesOf(ClassOf("java.util.ArrayList".v)))
    }

    it("should decompile: superClassesOf(classOf('java.util.ArrayList'))") {
      val model = SuperClassesOf(ClassOf("java.util.ArrayList".v))
      assert(model.toSQL == """superClassesOf(classOf("java.util.ArrayList"))""")
    }

    it("should execute: superClassesOf(classOf('java.util.ArrayList'))") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|superClassesOf(classOf('java.util.ArrayList'))
           |""".stripMargin)
      assert(Option(result).collect { case a: Array[_] => a.toList } contains
        Seq("java.util.AbstractList", "java.util.AbstractCollection", "java.lang.Object").map(Class.forName))
    }

    it("should execute: superClassesOf(new `java.util.ArrayList`())") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|superClassesOf(new `java.util.ArrayList`())
           |""".stripMargin)
      assert(Option(result).collect { case a: Array[_] => a.toList } contains
        Seq("java.util.AbstractList", "java.util.AbstractCollection", "java.lang.Object").map(Class.forName))
    }

  }

}