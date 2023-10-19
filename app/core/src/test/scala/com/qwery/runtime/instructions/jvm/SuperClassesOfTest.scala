package com.qwery.runtime.instructions.jvm

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class SuperClassesOfTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

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
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|superClassesOf(classOf('java.util.ArrayList'))
           |""".stripMargin)
      assert(Option(result).collect { case a: Array[_] => a.toList } contains
        Seq("java.util.AbstractList", "java.util.AbstractCollection", "java.lang.Object").map(Class.forName))
    }

    it("should execute: superClassesOf(new `java.util.ArrayList`())") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|superClassesOf(new `java.util.ArrayList`())
           |""".stripMargin)
      assert(Option(result).collect { case a: Array[_] => a.toList } contains
        Seq("java.util.AbstractList", "java.util.AbstractCollection", "java.lang.Object").map(Class.forName))
    }

  }

}