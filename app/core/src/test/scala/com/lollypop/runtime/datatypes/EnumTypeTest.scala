package com.lollypop.runtime.datatypes

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.Scope

class EnumTypeTest extends DataTypeFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = Scope()

  describe(EnumType.getClass.getSimpleName) {

    it("should encode/decode EnumType values") {
      verifyCodec(EnumType(Seq("Apple", "Banana", "Lemon", "Orange")), "Banana")
    }

    it("should resolve 'Enum ( male, female, unspecified )'") {
      verifySpec(spec = "Enum ( male, female, unspecified )", expected = EnumType(Seq("male", "female", "unspecified")))
    }

    it("should resolve 'Enum ( male, female, unspecified )[25]'") {
      verifySpec(spec = "Enum ( male, female, unspecified )[25]", expected = ArrayType(EnumType(Seq("male", "female", "unspecified")), capacity = Some(25)))
    }

    it("should provide a SQL representation") {
      verifySQL("Enum(Apple, Banana, Lemon, Orange)", EnumType(Seq("Apple", "Banana", "Lemon", "Orange")))
    }

  }

}
