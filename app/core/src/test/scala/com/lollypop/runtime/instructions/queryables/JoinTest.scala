package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.implicits._
import com.lollypop.language.{Template, _}
import com.lollypop.runtime.implicits.risky._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.conditions.AND
import com.lollypop.runtime.instructions.expressions.{Infix, JoinFieldRef}
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * join Test Suite
 */
class JoinTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[CrossJoin].getSimpleName) {

    it("should decompile select w/cross join") {
      verify(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |cross join CustomerAddresses as CA on CA.customerId is C.customerId
           |cross join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
    }

    it("should support select w/cross join") {
      val results = compiler.compile(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |cross join CustomerAddresses as CA on CA.customerId is C.customerId
           |cross join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
      assert(results == Select(fields = List(
        Infix("C".f, "id".f), Infix("C".f, "firstName".f), Infix("C".f, "lastName".f),
        Infix("A".f, "city".f), Infix("A".f, "state".f), Infix("A".f, "zipCode".f)),
        from = DatabaseObjectRef("Customers").as("C"),
        joins = List(
          CrossJoin(DatabaseObjectRef("CustomerAddresses").as("CA"), JoinFieldRef("CA", "customerId") is JoinFieldRef("C", "customerId")),
          CrossJoin(DatabaseObjectRef("Addresses").as("A"), JoinFieldRef("A", "addressId") is JoinFieldRef("CA", "addressId"))
        ),
        where = AND(Infix("C".f, "firstName".f) is "Lawrence", Infix("C".f, "lastName".f) is "Daniels")
      ))
    }

  }

  describe(classOf[InnerJoin].getSimpleName) {

    it("should interpret very complex patterns") {
      val template = Template(Select.templateCard)
      val params = template.processWithDebug(
        """|select C.customer_id, A.address_id
           |from Customers as C
           |inner join CustomerAddresses as CA on CA.customer_id = C.customer_id
           |inner join Addresses as A on A.address_id = CA.address_id
           |""".stripMargin)
      params.all foreach { case (k, v) => logger.info(s"$k -> $v") }
    }

    it("should decompile select w/inner join") {
      verify(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |inner join CustomerAddresses as CA on CA.customerId is C.customerId
           |inner join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
    }

    it("should support select w/inner join") {
      val results = compiler.compile(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |inner join CustomerAddresses as CA on CA.customerId is C.customerId
           |inner join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
      assert(results == Select(fields = List(
        Infix("C".f, "id".f), Infix("C".f, "firstName".f), Infix("C".f, "lastName".f),
        Infix("A".f, "city".f), Infix("A".f, "state".f), Infix("A".f, "zipCode".f)),
        from = DatabaseObjectRef("Customers").as("C"),
        joins = List(
          InnerJoin(DatabaseObjectRef("CustomerAddresses").as("CA"), JoinFieldRef("CA", "customerId") is JoinFieldRef("C", "customerId")),
          InnerJoin(DatabaseObjectRef("Addresses").as("A"), JoinFieldRef("A", "addressId") is JoinFieldRef("CA", "addressId"))
        ),
        where = AND(Infix("C".f, "firstName".f) is "Lawrence", Infix("C".f, "lastName".f) is "Daniels")
      ))
    }

    it("should support inner join (equal number of rows; left and right)") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|namespace 'samples.stocks'
           |drop if exists stockQuotes_A
           |create table stockQuotes_A (symbol: String(32), exchange: String(32), lastSale: Double)
           |insert into stockQuotes_A (symbol, exchange, lastSale)
           |values ('GREED', 'NASDAQ', 2345.78), ('BFG', 'NYSE', 113.56),
           |       ('ABC', 'AMEX', 11.46), ('ACME', 'NYSE', 56.78)
           |create index stockQuotes_A#symbol
           |
           |drop if exists companies_A
           |create table companies_A (symbol: String(32), name String(32))
           |insert into companies_A (symbol, name)
           |values ('ABC', 'ABC & Co'), ('BFG', 'BFG Corp.'),
           |       ('GREED', 'GreedIsGood.com'), ('ACME', 'ACME Inc.')
           |create index companies_A#symbol
           |
           |select B.name, A.symbol, A.exchange, A.lastSale
           |from stockQuotes_A as A
           |inner join companies_A as B on A.symbol is B.symbol
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("name" -> "ABC & Co", "exchange" -> "AMEX", "symbol" -> "ABC", "lastSale" -> 11.46),
        Map("name" -> "ACME Inc.", "exchange" -> "NYSE", "symbol" -> "ACME", "lastSale" -> 56.78),
        Map("name" -> "BFG Corp.", "exchange" -> "NYSE", "symbol" -> "BFG", "lastSale" -> 113.56),
        Map("name" -> "GreedIsGood.com", "exchange" -> "NASDAQ", "symbol" -> "GREED", "lastSale" -> 2345.78)
      ))
    }

    it("should support inner join (more rows on right)") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|namespace 'samples.stocks'
           |drop if exists stockQuotes_B
           |create table stockQuotes_B (symbol: String(32), exchange: String(32), lastSale: Double)
           |insert into stockQuotes_B (symbol, exchange, lastSale)
           |values ('ABC', 'AMEX', 11.46), ('ACME', 'NYSE', 56.78), ('BFG', 'NYSE', 113.56),
           |       ('GREED', 'NASDAQ', 2345.78)
           |create index stockQuotes_B#symbol
           |
           |drop if exists companies_B
           |create table companies_B (symbol: String(32), name String(32))
           |insert into companies_B (symbol, name)
           |values ('ABC', 'ABC & Co'), ('ACME', 'ACME Inc.'), ('BFG', 'BFG Corp.'),
           |       ('CATS', 'CATS4Life Inc.'), ('DOGS', 'DOGS4Ever Inc.'), ('GREED', 'GreedIsGood.com')
           |create index companies_B#symbol
           |
           |select B.name, A.symbol, A.exchange, A.lastSale
           |from stockQuotes_B as A
           |inner join companies_B as B on A.symbol is B.symbol
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("name" -> "ABC & Co", "exchange" -> "AMEX", "symbol" -> "ABC", "lastSale" -> 11.46),
        Map("name" -> "ACME Inc.", "exchange" -> "NYSE", "symbol" -> "ACME", "lastSale" -> 56.78),
        Map("name" -> "BFG Corp.", "exchange" -> "NYSE", "symbol" -> "BFG", "lastSale" -> 113.56),
        Map("name" -> "GreedIsGood.com", "exchange" -> "NASDAQ", "symbol" -> "GREED", "lastSale" -> 2345.78)
      ))
    }

    it("should support inner join  (more rows on left)") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|namespace 'samples.stocks'
           |drop if exists stockQuotes_C
           |create table stockQuotes_C (symbol: String(32), exchange: String(32), lastSale: Double)
           |insert into stockQuotes_C (symbol, exchange, lastSale)
           |values ('ABC', 'AMEX', 11.46), ('ACME', 'NYSE', 56.78), ('BFG', 'NYSE', 113.56),
           |       ('CATS', 'OTCBB', 0.007), ('DOGS', 'OTCBB', 0.007), ('GREED', 'NASDAQ', 2345.78)
           |create index stockQuotes_C#symbol
           |
           |drop if exists companies_C
           |create table companies_C (symbol: String(32), name String(32))
           |insert into companies_C (symbol, name)
           |values ('ABC', 'ABC & Co'), ('BFG', 'BFG Corp.'), ('DOGS', 'DOGS4Ever Inc.'),
           |       ('GREED', 'GreedIsGood.com')
           |create index companies_C#symbol
           |
           |select B.name, A.symbol, A.exchange, A.lastSale
           |from stockQuotes_C as A
           |inner join companies_C as B on A.symbol is B.symbol
           |""".stripMargin
      )
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("name" -> "ABC & Co", "exchange" -> "AMEX", "symbol" -> "ABC", "lastSale" -> 11.46),
        Map("name" -> "BFG Corp.", "exchange" -> "NYSE", "symbol" -> "BFG", "lastSale" -> 113.56),
        Map("name" -> "DOGS4Ever Inc.", "exchange" -> "OTCBB", "symbol" -> "DOGS", "lastSale" -> 0.007),
        Map("name" -> "GreedIsGood.com", "exchange" -> "NASDAQ", "symbol" -> "GREED", "lastSale" -> 2345.78)
      ))
    }

  }

  describe(classOf[FullOuterJoin].getSimpleName) {

    it("should support decompiling select w/full join") {
      verify(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |full join CustomerAddresses as CA on CA.customerId is C.customerId
           |full join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
    }

    it("should support select w/full join") {
      val results = compiler.compile(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |full join CustomerAddresses as CA on CA.customerId is C.customerId
           |full join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
      assert(results == Select(fields = List(
        Infix("C".f, "id".f), Infix("C".f, "firstName".f), Infix("C".f, "lastName".f),
        Infix("A".f, "city".f), Infix("A".f, "state".f), Infix("A".f, "zipCode".f)),
        from = DatabaseObjectRef("Customers").as("C"),
        joins = List(
          FullOuterJoin(DatabaseObjectRef("CustomerAddresses").as("CA"), JoinFieldRef("CA", "customerId") is JoinFieldRef("C", "customerId")),
          FullOuterJoin(DatabaseObjectRef("Addresses").as("A"), JoinFieldRef("A", "addressId") is JoinFieldRef("CA", "addressId"))
        ),
        where = AND(Infix("C".f, "firstName".f) is "Lawrence", Infix("C".f, "lastName".f) is "Daniels")
      ))
    }

  }

  describe(classOf[LeftOuterJoin].getSimpleName) {

    it("should support decompiling select w/left join") {
      verify(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |left join CustomerAddresses as CA on CA.customerId is C.customerId
           |left join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
    }

    it("should support select w/left join") {
      val results = compiler.compile(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |left join CustomerAddresses as CA on CA.customerId is C.customerId
           |left join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
      assert(results == Select(fields = List(
        Infix("C".f, "id".f), Infix("C".f, "firstName".f), Infix("C".f, "lastName".f),
        Infix("A".f, "city".f), Infix("A".f, "state".f), Infix("A".f, "zipCode".f)),
        from = DatabaseObjectRef("Customers").as("C"),
        joins = List(
          LeftOuterJoin(DatabaseObjectRef("CustomerAddresses").as("CA"), JoinFieldRef("CA", "customerId") is JoinFieldRef("C", "customerId")),
          LeftOuterJoin(DatabaseObjectRef("Addresses").as("A"), JoinFieldRef("A", "addressId") is JoinFieldRef("CA", "addressId"))
        ),
        where = AND(Infix("C".f, "firstName".f) is "Lawrence", Infix("C".f, "lastName".f) is "Daniels")
      ))
    }

  }

  describe(classOf[RightOuterJoin].getSimpleName) {

    it("should support decompiling select w/right join") {
      verify(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |right join CustomerAddresses as CA on CA.customerId is C.customerId
           |right join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
    }

    it("should support select w/right join") {
      val results = compiler.compile(
        """|select C.id, C.firstName, C.lastName, A.city, A.state, A.zipCode
           |from Customers as C
           |right join CustomerAddresses as CA on CA.customerId is C.customerId
           |right join Addresses as A on A.addressId is CA.addressId
           |where C.firstName is 'Lawrence' and C.lastName is 'Daniels'
           |""".stripMargin)
      assert(results == Select(fields = List(
        Infix("C".f, "id".f), Infix("C".f, "firstName".f), Infix("C".f, "lastName".f),
        Infix("A".f, "city".f), Infix("A".f, "state".f), Infix("A".f, "zipCode".f)),
        from = DatabaseObjectRef("Customers").as("C"),
        joins = List(
          RightOuterJoin(DatabaseObjectRef("CustomerAddresses").as("CA"), JoinFieldRef("CA", "customerId") is JoinFieldRef("C", "customerId")),
          RightOuterJoin(DatabaseObjectRef("Addresses").as("A"), JoinFieldRef("A", "addressId") is JoinFieldRef("CA", "addressId"))
        ),
        where = AND(Infix("C".f, "firstName".f) is "Lawrence", Infix("C".f, "lastName".f) is "Daniels")
      ))
    }

  }

}
