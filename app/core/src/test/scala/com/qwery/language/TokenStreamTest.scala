package com.qwery.language

import com.qwery.language.Token.{AtomToken, NumericToken, OperatorToken, QuotedToken}
import com.qwery.runtime.QweryCompiler
import org.scalatest.funspec.AnyFunSpec

class TokenStreamTest extends AnyFunSpec {

  describe(classOf[TokenStream].getSimpleName) {

    it("supports captureIf(...)") {
      val ts = TokenStream("(this could be code)")
      val list = ts.captureIf("(", ")", delimiter = Some(","))(_.peek.map(_.valueAsString).getOrElse(""))
      assert(list == List("this", "could", "be", "code"))
    }

    it("supports foldWhile(...)") {
      val ts = TokenStream("""(n => "Hello World"[n])(5)""")
      assert(ts.nextIf("("))
      val list = ts.foldWhile[List[Any]](Nil)(_.value != ")") { case (list, token) =>
        token.value :: list
      }
      assert(ts.nextIf(")"))
      assert(list.reverse == List("n", "=>", "Hello World", "[", "n", "]"))
    }

    it("supports get(...)") {
      val ts = TokenStream("""(n => "Hello World"[n])(5)""")
      val tokens = for {
        a <- ts.get { case OperatorToken((t, _, _)) if t == "(" => t }
        b <- ts.get { case t: AtomToken => t.value } // n
        c <- ts.get { case OperatorToken((t, _, _)) if t == "=>" => t }
        d <- ts.get { case t: QuotedToken => t.value } // "Hello World"
        e <- ts.get { case OperatorToken((t, _, _)) if t == "[" => t }
        f <- ts.get { case t: AtomToken => t.value } // n
        g <- ts.get { case OperatorToken((t, _, _)) if t == "]" => t }
        h <- ts.get { case OperatorToken((t, _, _)) if t == ")" => t }
        i <- ts.get { case OperatorToken((t, _, _)) if t == "(" => t }
        j <- ts.get { case t: NumericToken => t.value } // 5
        k <- ts.get { case OperatorToken((t, _, _)) if t == ")" => t }
      } yield List(a, b, c, d, e, f, g, h, i, j, k)
      assert(tokens contains List("(", "n", "=>", "Hello World", "[", "n", "]", ")", "(", 5, ")"))
    }

    it("supports matches(...)") {
      implicit val compiler: QweryCompiler = QweryCompiler()
      val ts = TokenStream("(x: Int): Int => x + 1")
      assert(ts.getPosition == 0)
      assert((ts is "{ (") || (ts is "(") || (ts matches "%FP:params ?: +?%T:returnType => %i:code"))
    }

    it("supports skip(...)") {
      val ts = TokenStream("(this could be code)")
      ts.skip(2)
      assert(ts is "could")
    }

    it("should identify '(x: Int) => x + 1' as a lambda function") {
      implicit val compiler: QweryCompiler = QweryCompiler()
      val ts = TokenStream("(x: Int) => x + 1")
      assert(ts.getPosition == 0)
      assert(ts matches "%FP:params ?: +?%T:returnType => %i:code")
      assert(ts.getPosition == 0)
    }

    it("should identify '(x: Int): Int => x + 1' as a lambda function") {
      implicit val compiler: QweryCompiler = QweryCompiler()
      val ts = TokenStream("(x: Int): Int => x + 1")
      assert(ts.getPosition == 0)
      assert(ts matches "%FP:params ?: +?%T:returnType => %i:code")
      assert(ts.getPosition == 0)
    }

  }

}
