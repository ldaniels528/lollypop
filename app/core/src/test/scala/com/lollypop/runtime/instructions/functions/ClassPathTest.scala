package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL
import com.lollypop.runtime.{LollypopCompiler, Scope}
import org.scalatest.funspec.AnyFunSpec

class ClassPathTest extends AnyFunSpec {

  describe(classOf[ClassPath].getSimpleName) {

    it("should return classes from the class path") {
      val classes = ClassPath.searchClassPath(pattern = "akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)")
      assert(classes.mkString("\n") ==
        """|akka.dispatch.BoundedDequeBasedMessageQueueSemantics
           |akka.dispatch.DequeBasedMessageQueueSemantics
           |akka.dispatch.UnboundedDequeBasedMessageQueueSemantics
           |""".stripMargin.trim)
    }

    it("should compile: classPath('akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)')") {
      val model = LollypopCompiler().compile(
        """|classPath("akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)")
           |""".stripMargin)
      assert(model == ClassPath(pattern = "akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)".v))
    }

    it("should decompile: classPath('akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)')") {
      val model = ClassPath(pattern = "akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)".v)
      assert(model.toSQL == """classPath("akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)")""")
    }

    it("should execute: transpose(classPath('akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)'))") {
      val (_, _, rc) =
        """|transpose(className: classPath("akka[.]dispatch[.](.*)D(.*)M(.*)S(.*)"))
           |""".stripMargin.searchSQL(Scope())
      assert(rc.toMapGraph == List(
        Map("className" -> "akka.dispatch.BoundedDequeBasedMessageQueueSemantics"),
        Map("className" -> "akka.dispatch.DequeBasedMessageQueueSemantics"),
        Map("className" -> "akka.dispatch.UnboundedDequeBasedMessageQueueSemantics")
      ))
    }

  }

}
