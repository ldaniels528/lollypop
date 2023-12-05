package com.lollypop.language

import org.scalatest.funspec.AnyFunSpec

class LollypopUniverseTest extends AnyFunSpec {

  describe(classOf[LollypopUniverse].getSimpleName) {

    it("should provide a root scope with pre-populated values") {
      val ctx = LollypopUniverse()
      val scope = ctx.createRootScope()
      assert(scope.resolveAs[Double]("π") contains Math.PI)
    }

    it("should provide the logging services") {
      val ctx0 = LollypopUniverse(isServerMode = true)
      val ctx = ctx0
        .withDebug(s => ctx0.system.stdOut.writer.println(s))
        .withError(s => ctx0.system.stdErr.writer.println(s))
        .withInfo(s => ctx0.system.stdOut.writer.println(s))
        .withWarn(s => ctx0.system.stdErr.writer.println(s))

      ctx.debug("debug: Hello World")
      ctx.info("info: Hello World")
      ctx.warn("warn: Hello World")
      ctx.error("error: Hello World")
      assert(ctx.system.stdOut.asString() == "debug: Hello World\ninfo: Hello World\n")
      assert(ctx.system.stdErr.asString() == "warn: Hello World\nerror: Hello World\n")
    }

  }

}
