package com.lollypop.repl

import com.lollypop.runtime.{LollypopCompiler, Scope}
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class LollypopCLITest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[LollypopCLI].getSimpleName) {

    it("should execute inline scripts") {
      val scope = LollypopCLI.interact(Scope(), console = {
        val it = Seq(
          "set x = 3",
          "set y = 6",
          "set z = x + y"
        ).iterator
        () => if (it.hasNext) it.next() else "exit"
      })
      scope.toRowCollection.tabulate().foreach(Console.println)
      assert(scope.resolve("z") contains 9)
    }

    it("should execute a .sql file via runScript(...)") {
      val scope: Scope = Scope()
      val outputFile = new File("./vin-mapping.json")
      assert(!outputFile.exists() || outputFile.delete())
      LollypopCLI.runScript(scope, "./app/examples/src/main/lollypop/GenerateVinMapping.sql")
      assert(outputFile.exists())
    }

    it("should execute a .sql file via main(...)") {
      val outputFile = new File("./vin-mapping.json")
      assert(!outputFile.exists() || outputFile.delete())
      LollypopCLI.main(Array("./app/examples/src/main/lollypop/GenerateVinMapping.sql"))
      assert(outputFile.exists())
    }

  }

}
