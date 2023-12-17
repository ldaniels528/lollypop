package com.lollypop.language

import com.lollypop.language.models.Literal.implicits.NumericLiteralTokenStreamExtensions
import org.scalatest.funspec.AnyFunSpec

/**
  * Token Stream Extensions Tests
  * @author lawrence.daniels@gmail.com
  */
class TokenStreamExtensionsTest extends AnyFunSpec {

  describe(classOf[TokenStreamExtensions].getSimpleName) {

    it("""should identify "100" as a constant""") {
      assert(TokenStream("100 = 1").isConstant)
    }

    it("""should identify "'Hello World'" as a constant""") {
      assert(TokenStream("'Hello World' = -1").isConstant)
    }

    it("""should identify "`Symbol`" as a field""") {
      assert(TokenStream("`Symbol` = 100").isField)
    }

    it("""should identify "PROPERTY_VAL" as a field""") {
      assert(TokenStream("PROPERTY_VAL = 100").isField)
    }

    it("""should identify "Sum(A.Symbol)" as a function""") {
      assert(TokenStream("Sum(A.Symbol) = 100").isFunctionCall)
    }

    it("""should not identify "ABC + (1 * x)" as a function""") {
      assert(!TokenStream("ABC + (1 * x)").isFunctionCall)
    }

    it("should support identifiers containing $ symbols") {
      assert(TokenStream("$$DATA_SOURCE_ID").isIdentifier)
    }

    it("should match multiple tokens (is)") {
      val ts = TokenStream("Now is the Winter of our discontent!")
      assert(ts is "Now")
      assert(ts is "Now is")
      assert(ts is "Now is the Winter of our discontent !")
    }

    it("should match multiple tokens (nextIf)") {
      val ts = TokenStream("left OUTER join")
      assert((ts nextIf "left OUTER join") && ts.isEmpty)
    }

  }

}

