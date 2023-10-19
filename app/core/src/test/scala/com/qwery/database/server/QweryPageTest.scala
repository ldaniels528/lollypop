package com.qwery.database.server

import com.qwery.runtime.Scope
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class QweryPageTest extends AnyFunSpec {

  describe(classOf[QweryPage].getSimpleName) {

    it("should evaluate a simple markdown document as HTML") {
      val page = QweryPage(
        new File("./temp"),
        """|QweryPages Example
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
           |<QweryCode>
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
           |</QweryCode>
           |""".stripMargin)
      val output = page.evaluate()(Scope())
      assert(output ==
        """|<!DOCTYPE html>
           |<html>
           |<head>
           |<title>Qwery Pages</title>
           |
           |</head>
           |<h1>QweryPages Example</h1>
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
      val page = QweryPage(
        new File("./temp"),
        """|QweryPages Example
           |==================
           |## Stock Quotes
           |<QweryExample>
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
           |</QweryExample>
           |""".stripMargin)
      val output = page.evaluate()(Scope())
      assert(output ===
        """|<!DOCTYPE html>
           |<html>
           |<head>
           |<title>Qwery Pages</title>
           |
           |</head>
           |<h1>QweryPages Example</h1>
           |<h2>Stock Quotes</h2>
           |<div class="source-code">
           |<pre><code class="language-qwery">from (
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
