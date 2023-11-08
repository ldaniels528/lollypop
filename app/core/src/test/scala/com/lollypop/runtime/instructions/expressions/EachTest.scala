package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.CodeBlock
import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec

class EachTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Each].getSimpleName) {

    it("should support compiling each .. in") {
      val results = compiler.compile(
        """|each stock in (select symbol, lastSale from Securities where naics == '12345') {
           |  stdout <=== '{{symbol}} is {{lastSale}}/share'
           |}
           |""".stripMargin)
      assert(results == Each(
        variable = "stock",
        queryable = Select(fields = Seq("symbol".f, "lastSale".f), from = DatabaseObjectRef("Securities"), where = "naics".f === "12345"),
        code = CodeBlock(
          TransferFrom("stdout".f, "{{symbol}} is {{lastSale}}/share")
        )
      ))
    }

    it("should support decompiling each .. in") {
      verify(
        """|each stock in (select symbol, lastSale from Securities where naics == '12345') {
           |  stdout <=== '{{@stock.symbol}} is {{@stock.lastSale}}/share'
           |}
           |""".stripMargin)
    }

    it(s"should support executing each .. in") {
      LollypopVM.executeSQL(Scope(),
        s"""|declare table travelers(uid UUID, lastName String(12), firstName String(12), destAirportCode String(3))
            |insert into @travelers (uid, lastName, firstName, destAirportCode)
            |values
            | ('71f67307-71ce-44d2-a2cf-2d29a536ad39', 'JONES', 'GARRY', 'SNA'),  ('eab5bfa4-fb9f-4115-b837-deff101d2a19', 'JONES', 'DEBBIE', 'SNA'),
            | ('333025a0-4b62-4170-9664-f1abe82d933a', 'JONES', 'TAMERA', 'SNA'), ('63a04193-5446-49de-859c-6ebda6615d4f', 'JONES', 'ERIC', 'SNA'),
            | ('1bed9d30-b391-4796-b9fb-79fff6746fe2', 'ADAMS', 'KAREN', 'DTW'),  ('686060d6-eec7-46b0-ba37-9c45d55d1ea3', 'ADAMS', 'MIKE', 'DTW'),
            | ('2a33228d-06a7-4d7c-921f-00dad33c19fb', 'JONES', 'SAMANTHA', 'BUR'),('fffe8f51-6ebd-427b-a612-4ed0c875fc93', 'SHARMA', 'PANKAJ', 'LAX')
            | 
            |each traveler in (select * from @travelers limit 5) {
            |  stdout <=== '{{ uid }} is {{ lastName }}, {{ firstName }}'
            |}
            |""".stripMargin)
    }

  }

}
