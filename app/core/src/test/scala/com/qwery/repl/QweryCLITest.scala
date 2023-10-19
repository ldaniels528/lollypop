package com.qwery.repl

import com.qwery.runtime.{QweryCompiler, Scope}
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class QweryCLITest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[QweryCLI].getSimpleName) {

    it("should execute inline scripts") {
      val scope = QweryCLI.interact(Scope(), console = {
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
      QweryCLI.runScript(scope, "./contrib/examples/src/main/qwery/GenerateVinMapping.sql")
      assert(outputFile.exists())
    }

    it("should execute a .sql file via main(...)") {
      val outputFile = new File("./vin-mapping.json")
      assert(!outputFile.exists() || outputFile.delete())
      QweryCLI.main(Array("./contrib/examples/src/main/qwery/GenerateVinMapping.sql"))
      assert(outputFile.exists())
    }

  }

}
