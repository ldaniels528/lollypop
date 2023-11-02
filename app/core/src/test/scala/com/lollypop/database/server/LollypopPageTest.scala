package com.lollypop.database.server

import com.lollypop.runtime.Scope
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class LollypopPageTest extends AnyFunSpec {

  describe(classOf[LollypopPage].getSimpleName) {

    it("should evaluate a simple markdown document as HTML") {
      val page = LollypopPage(
        new File("./temp"),
        """|LollypopPages Example
           |==================
           |## Stock Quotes
           |```sql
           |from (
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    |------------------------------|
           |) where lastSale < 75 order by lastSale
           |```
           |<LollypopCode>
           |from (
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    |------------------------------|
           |) where lastSale < 75 order by lastSale
           |</LollypopCode>
           |""".stripMargin)
      val output = page.execute()(Scope())._3
      assert(output ==
        """|<!DOCTYPE html>
           |<html>
           |<head>
           |<title>Lollypop Pages</title>
           |
           |</head>
           |<h1>LollypopPages Example</h1>
           |<h2>Stock Quotes</h2>
           |<pre><code class="language-sql">from (
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    |------------------------------|
           |) where lastSale &lt; 75 order by lastSale
           |</code></pre>
           |<pre><code class="console-result">
           ||------------------------------|
           || symbol | exchange | lastSale |
           ||------------------------------|
           || XYZ    | AMEX     |    31.95 |
           || AAXX   | NYSE     |    56.12 |
           ||------------------------------|</code></pre>
           |</html>
           |""".stripMargin.trim)
    }

    it("should echo and evaluate code then generate HTML") {
      val page = LollypopPage(
        new File("./temp"),
        """|LollypopPages Example
           |==================
           |## Stock Quotes
           |<LollypopExample>
           |from (
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    |------------------------------|
           |) where lastSale < 75 order by lastSale
           |</LollypopExample>
           |""".stripMargin)
      val output = page.execute()(Scope())._3
      assert(output ===
        """|<!DOCTYPE html>
           |<html>
           |<head>
           |<title>Lollypop Pages</title>
           |
           |</head>
           |<h1>LollypopPages Example</h1>
           |<h2>Stock Quotes</h2>
           |<div class="source-code">
           |<pre><code class="language-lollypop">from (
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    |------------------------------|
           |) where lastSale < 75 order by lastSale</code></pre>
           |</div>
           |<h4>Results</h4><pre><code class="console-result">
           ||------------------------------|
           || symbol | exchange | lastSale |
           ||------------------------------|
           || XYZ    | AMEX     |    31.95 |
           || AAXX   | NYSE     |    56.12 |
           ||------------------------------|</code></pre>
           |</html>
           |""".stripMargin.trim)
    }

  }

}
