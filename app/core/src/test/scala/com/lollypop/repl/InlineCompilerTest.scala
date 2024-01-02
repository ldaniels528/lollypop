package com.lollypop.repl

class InlineCompilerTest extends REPLFunSpec {
  private val inlineCompiler = new InlineCompiler {}

  describe(classOf[InlineCompiler].getSimpleName) {

    it("should execute inline scripts") {
      val scope = inlineCompiler.compileOnly(createRootScope(), console = {
        val it = Seq(
          "ls app/examples where not isHidden order by length desc limit 5",
          "cd ~/Downloads/images",
          """"Hello World 123" matches "H(.*) W(.*) \d+"""",
          "",
          ""
        ).iterator
        () => if (it.hasNext) it.next() else ""
      }, showPrompt = () => print("-> "))
      println()
      scope.toRowCollection.tabulate().foreach(println)
    }

  }

}
