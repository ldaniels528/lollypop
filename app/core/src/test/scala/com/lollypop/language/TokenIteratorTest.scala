package com.lollypop.language

import com.lollypop.language.Token._
import org.scalatest.funspec.AnyFunSpec

/**
 * Token Iterator Test
 * @author lawrence.daniels@gmail.com
 */
class TokenIteratorTest extends AnyFunSpec {

  describe(classOf[TokenIterator].getSimpleName) {

    it("supports operators and symbols") {
      val tok = TokenIterator("~!@#$%^&*()-_=+[]{}|\\;:,.< >?/")
      assert(tok.toList == List(
        OperatorToken("~"), OperatorToken("!"), OperatorToken("@"), OperatorToken("#"), OperatorToken("$"),
        OperatorToken("%"), OperatorToken("^"), OperatorToken("&"), OperatorToken("*"), OperatorToken("("),
        OperatorToken(")"), OperatorToken("-"), AlphaNumericToken("_"), OperatorToken("="), OperatorToken("+"),
        OperatorToken("["), OperatorToken("]"), OperatorToken("{"), OperatorToken("}"), OperatorToken("|"),
        OperatorToken("\\"), OperatorToken(";"), OperatorToken(":"), OperatorToken(","), OperatorToken("."),
        OperatorToken("<"), OperatorToken(">"), OperatorToken("?"), OperatorToken("/")
      ))
    }

    it("supports parsing arithmetic expressions") {
      val tok = TokenIterator("-(7 + 9)")
      assert(tok.toList == List(
        OperatorToken("-"), OperatorToken("("), NumericToken("7"), OperatorToken("+"), NumericToken("9"), OperatorToken(")")
      ))
    }

    it("supports an unclosed backticks quote (Hello`)") {
      val tok = TokenIterator("Hello`")
      assert(tok.toList == List(AlphaNumericToken("Hello"), BackticksQuotedToken("`", "`")))
    }

    it("supports an unclosed backticks quote (`Hello)") {
      val tok = TokenIterator("`Hello")
      assert(tok.toList == List(BackticksQuotedToken("`Hello", "`Hello")))
    }

    it("supports an unclosed double quote (Hello\")") {
      val tok = TokenIterator("Hello\"")
      assert(tok.toList == List(AlphaNumericToken("Hello"), DoubleQuotedToken("\"", "\"")))
    }

    it("supports an unclosed double quote (\"Hello)") {
      val tok = TokenIterator("\"Hello")
      assert(tok.toList == List(DoubleQuotedToken("\"Hello", "\"Hello")))
    }

    it("supports an unclosed single quote (Hello')") {
      val tok = TokenIterator("Hello'")
      assert(tok.toList == List(AlphaNumericToken("Hello"), SingleQuotedToken("'", "'")))
    }

    it("supports an unclosed single quote ('Hello)") {
      val tok = TokenIterator("'Hello")
      assert(tok.toList == List(SingleQuotedToken("'Hello", "'Hello")))
    }

    it("supports parsing triple-backtick-quoted strings") {
      val tok = TokenIterator(
        """|(```scala
           |var count = 0
           |while(count < 5) {
           |  println(s"Hello $count")
           |  count += 1
           |}
           |count
           |```)
           |""".stripMargin)
      val results = tok.toList
      assert(results == List(
        OperatorToken("("),
        TripleBackticksQuotedToken(
          text =
            """|```scala
               |var count = 0
               |while(count < 5) {
               |  println(s"Hello $count")
               |  count += 1
               |}
               |count
               |```""".stripMargin,
          value =
            """|scala
               |var count = 0
               |while(count < 5) {
               |  println(s"Hello $count")
               |  count += 1
               |}
               |count
               |""".stripMargin),
        OperatorToken(")")))
    }

    it("supports parsing triple-double-quoted strings") {
      val tok = TokenIterator("value is\n \"\"\"\"Hello\" \"World\"\\n\"\"\"")
      assert(tok.toList == List(
        AlphaNumericToken("value"),
        AlphaNumericToken("is"),
        TripleDoubleQuotedToken(text = "\"\"\"\"Hello\" \"World\"\\n\"\"\"", value = "\"Hello\" \"World\"\\n")))
    }

    it("supports parsing triple-single-quoted strings with inner quotes") {
      val tok = TokenIterator("value is\n ''''Hello' 'World'\n'''")
      assert(tok.toList == List(
        AlphaNumericToken("value"),
        AlphaNumericToken("is"),
        TripleSingleQuotedToken(text = "''''Hello' 'World'\n'''", value = "'Hello' 'World'\n")
      ))
    }

    it("supports parsing function de-referencing expression") {
      val tok = TokenIterator("doSomething(...)")
      assert(tok.toList == List(
        AlphaNumericToken("doSomething"), OperatorToken("("), OperatorToken("..."), OperatorToken(")")
      ))
    }

    it("supports low-level parsing of select queries") {
      val tok = TokenIterator("select * from CompanyList where Industry == 'Oil/Gas Transmission' ")
      assert(tok.peek contains AlphaNumericToken("select"))
      assert(tok.toList == List(
        AlphaNumericToken("select"),
        OperatorToken("*"),
        AlphaNumericToken("from"),
        AlphaNumericToken("CompanyList"),
        AlphaNumericToken("where"),
        AlphaNumericToken("Industry"),
        OperatorToken("=="),
        SingleQuotedToken(text = "'Oil/Gas Transmission'", value = "Oil/Gas Transmission")
      ))
    }

    it("supports ASCII characters as identifiers (x²)") {
      val tok = TokenIterator("x²")
      assert(tok.toList == List(
        AlphaNumericToken("x²")
      ))
    }

  }

}

