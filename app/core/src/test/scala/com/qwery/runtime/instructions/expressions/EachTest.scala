package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.CodeBlock
import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import com.qwery.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class EachTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: QweryCompiler = QweryCompiler()

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

    it("should support compiling each .. in reverse") {
      val results = compiler.compile(
        """|each stock in reverse (select symbol, lastSale from Securities where naics == '12345') {
           |  stdout <=== '{{symbol}} is {{lastSale}}/share'
           |}
           |""".stripMargin)
      assert(results == Each(
        variable = "stock",
        queryable = Select(fields = Seq("symbol".f, "lastSale".f), from = DatabaseObjectRef("Securities"), where = "naics".f === "12345"),
        code = CodeBlock(
          TransferFrom("stdout".f, "{{symbol}} is {{lastSale}}/share")
        ),
        isReverse = true
      ))
    }

    it("should support decompiling each .. in") {
      verify(
        """|each stock in (select symbol, lastSale from Securities where naics == '12345') {
           |  stdout <=== '{{@stock.symbol}} is {{@stock.lastSale}}/share'
           |}
           |""".stripMargin)
    }

    it("should support decompiling each .. in reverse") {
      verify(
        """|each stock in reverse (select symbol, lastSale from Securities where naics == '12345') {
           |  stdout <=== '{{@stock.symbol}} is {{@stock.lastSale}}/share'
           |}
           |""".stripMargin)
    }

    it(s"should support executing each .. in") {
      QweryVM.executeSQL(Scope(),
        s"""|declare table travelers(uid UUID, lastName String(12), firstName String(12), destAirportCode String(3))
            |insert into @@travelers (uid, lastName, firstName, destAirportCode)
            |values
            | ('71f67307-71ce-44d2-a2cf-2d29a536ad39', 'JONES', 'GARRY', 'SNA'),  ('eab5bfa4-fb9f-4115-b837-deff101d2a19', 'JONES', 'DEBBIE', 'SNA'),
            | ('333025a0-4b62-4170-9664-f1abe82d933a', 'JONES', 'TAMERA', 'SNA'), ('63a04193-5446-49de-859c-6ebda6615d4f', 'JONES', 'ERIC', 'SNA'),
            | ('1bed9d30-b391-4796-b9fb-79fff6746fe2', 'ADAMS', 'KAREN', 'DTW'),  ('686060d6-eec7-46b0-ba37-9c45d55d1ea3', 'ADAMS', 'MIKE', 'DTW'),
            | ('2a33228d-06a7-4d7c-921f-00dad33c19fb', 'JONES', 'SAMANTHA', 'BUR'),('fffe8f51-6ebd-427b-a612-4ed0c875fc93', 'SHARMA', 'PANKAJ', 'LAX')
            | 
            |each traveler in (select * from @@travelers limit 5) {
            |  stdout <=== '{{ uid }} is {{ lastName }}, {{ firstName }}'
            |}
            |""".stripMargin)
    }

    it(s"should support executing each .. in reverse") {
      QweryVM.executeSQL(Scope(),
        s"""|declare table travelers(uid UUID, lastName String(12), firstName String(12), destAirportCode String(3))
            |insert into @@travelers (uid, lastName, firstName, destAirportCode)
            |values
            | ('71f67307-71ce-44d2-a2cf-2d29a536ad39', 'JONES', 'GARRY', 'SNA'),  ('eab5bfa4-fb9f-4115-b837-deff101d2a19', 'JONES', 'DEBBIE', 'SNA'),
            | ('333025a0-4b62-4170-9664-f1abe82d933a', 'JONES', 'TAMERA', 'SNA'), ('63a04193-5446-49de-859c-6ebda6615d4f', 'JONES', 'ERIC', 'SNA'),
            | ('1bed9d30-b391-4796-b9fb-79fff6746fe2', 'ADAMS', 'KAREN', 'DTW'),  ('686060d6-eec7-46b0-ba37-9c45d55d1ea3', 'ADAMS', 'MIKE', 'DTW'),
            | ('2a33228d-06a7-4d7c-921f-00dad33c19fb', 'JONES', 'SAMANTHA', 'BUR'),('fffe8f51-6ebd-427b-a612-4ed0c875fc93', 'SHARMA', 'PANKAJ', 'LAX')
            | 
            |each traveler in reverse (select * from @@travelers limit 5) {
            |  stdout <=== '{{ uid }} is {{ lastName }}, {{ firstName }}'
            |}
            |""".stripMargin)
    }

    it(s"should support executing each .. in reverse .. yield") {
      val (_, _, device) = QweryVM.searchSQL(Scope(), sql =
        s"""|declare table travelers(uid UUID, lastName String(12), firstName String(12), destAirportCode String(3))
            |insert into @@travelers (uid, lastName, firstName, destAirportCode)
            |values
            | ('71f67307-71ce-44d2-a2cf-2d29a536ad39', 'JONES', 'GARRY', 'SNA'),  ('eab5bfa4-fb9f-4115-b837-deff101d2a19', 'JONES', 'DEBBIE', 'SNA'),
            | ('333025a0-4b62-4170-9664-f1abe82d933a', 'JONES', 'TAMERA', 'SNA'), ('63a04193-5446-49de-859c-6ebda6615d4f', 'JONES', 'ERIC', 'SNA'),
            | ('1bed9d30-b391-4796-b9fb-79fff6746fe2', 'ADAMS', 'KAREN', 'DTW'),  ('686060d6-eec7-46b0-ba37-9c45d55d1ea3', 'ADAMS', 'MIKE', 'DTW'),
            | ('2a33228d-06a7-4d7c-921f-00dad33c19fb', 'JONES', 'SAMANTHA', 'BUR'),('fffe8f51-6ebd-427b-a612-4ed0c875fc93', 'SHARMA', 'PANKAJ', 'LAX')
            |
            |each traveler in reverse (from @@travelers) yield {
            |  select __id, firstName, lastName, destAirportCode
            |}
            |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("__id" -> 7, "firstName" -> "PANKAJ", "lastName" -> "SHARMA", "destAirportCode" -> "LAX"),
        Map("__id" -> 6, "firstName" -> "SAMANTHA", "lastName" -> "JONES", "destAirportCode" -> "BUR"),
        Map("__id" -> 5, "firstName" -> "MIKE", "lastName" -> "ADAMS", "destAirportCode" -> "DTW"),
        Map("__id" -> 4, "firstName" -> "KAREN", "lastName" -> "ADAMS", "destAirportCode" -> "DTW"),
        Map("__id" -> 3, "firstName" -> "ERIC", "lastName" -> "JONES", "destAirportCode" -> "SNA"),
        Map("__id" -> 2, "firstName" -> "TAMERA", "lastName" -> "JONES", "destAirportCode" -> "SNA"),
        Map("__id" -> 1, "firstName" -> "DEBBIE", "lastName" -> "JONES", "destAirportCode" -> "SNA"),
        Map("__id" -> 0, "firstName" -> "GARRY", "lastName" -> "JONES", "destAirportCode" -> "SNA")
      ))
    }

  }

}
