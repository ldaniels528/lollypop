package com.qwery.runtime.plastics

import com.qwery.language.QweryUniverse
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope, safeCast}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec

import java.time.{LocalDate, LocalTime}

class RuntimePlatformTest extends AnyFunSpec {
  private val ctx = QweryUniverse(isServerMode = true)

  describe("Any:isArray()") {
    it("should execute: [].isArray()")(isTrue("[].isArray()"))
    it("should execute: 'Hello'.isArray()")(isFalse("'Hello'.isArray()"))
    it("should decompile: 'Hello'.isArray()")(decompile(""""Hello".isArray()"""))
  }

  describe("Any:isDateTime()") {
    it("should execute: DateTime().isDateTime()")(isTrue("DateTime().isDateTime()"))
    it("should execute: '2022-08-21T12:57:53.564Z'.isDateTime()")(isTrue("'2022-08-21T12:57:53.564Z'.isDateTime()"))
    it("should execute: 5.isDateTime()")(isFalse("5.isDateTime()"))
    it("should execute: true.isDateTime()")(isFalse("true.isDateTime()"))
    it("should decompile: true.isDateTime()")(decompile("true.isDateTime()"))
  }

  describe("Any:isFunction()") {
    it("should execute: isFunction((n: Int) => n + 1)")(isTrue(
      """|val f = (n: Int) => n + 1
         |f.isFunction()
         |""".stripMargin))
    it("should execute: isFunction(true)")(isFalse(""""Hello".isFunction()"""))
    it("should decompile: true.isDateTime()")(decompile(""""Hello".isFunction()"""))
  }

  describe("Any:isISO8601()") {
    it("should execute: '2022-08-21T12:57:53.564Z'.isISO8601()")(isTrue("'2022-08-21T12:57:53.564Z'.isISO8601()"))
    it("should execute: [].isISO8601()")(isFalse("[].isISO8601()"))
    it("should execute: DateTime().isISO8601()")(isFalse("DateTime().isISO8601()"))
    it("should decompile: DateTime().isISO8601()")(decompile("DateTime().isISO8601()"))
  }

  describe("Any:isNumber()") {
    it("should execute: 'Hello World'.isNumber()")(isFalse("'Hello World'.isNumber()"))
    it("should execute: 5.isNumber()")(isTrue("5.isNumber()"))
    it("should execute: true.isNumber()")(isFalse("true.isNumber()"))
    it("should decompile: true.isNumber()")(decompile("true.isNumber()"))
  }

  describe("Any:isString()") {
    it("should execute: 'Hello World'.isString()")(isTrue("'Hello World'.isString()"))
    it("should execute: 5.isString()")(isFalse("5.isString()"))
    it("should execute: true.isString()")(isFalse("true.isString()"))
    it("should decompile: true.isString()")(decompile("true.isString()"))
  }

  describe("Any:isTable()") {
    it("should execute: Travelers.isTable()")(isTrue(
      """|declare table Travelers(id UUID, lastName String(12), firstName String(12), destAirportCode String(3))
         |Travelers.isTable()
         |""".stripMargin))
    it("should execute: 'Hello World'.isTable()")(isFalse("'Hello World'.isTable()"))
    it("should execute: 5.isTable()")(isFalse("5.isTable()"))
    it("should decompile: 5.isTable()")(decompile("5.isTable()"))
  }

  describe("Any:isUUID()") {
    it("should execute: UUID().isUUID()")(isTrue("UUID().isUUID()"))
    it("should execute: '71f67307-71ce-44d2-a2cf-2d29a536ad39'.isUUID()")(isTrue("'71f67307-71ce-44d2-a2cf-2d29a536ad39'.isUUID()"))
    it("should execute: 'Hello World'.isUUID()")(isFalse("'Hello World'.isUUID()"))
    it("should decompile: UUID().isUUID()")(decompile("UUID().isUUID()"))
  }


  describe("Any:toJson()") {
    it("should execute: [{Hello:'World'}].toJsonString()") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """[{Hello:'World'}].toJsonString()""")
      assert(results == """[{"Hello":"World"}]""")
    }
  }

  describe("Any:toJsonPretty()") {
    it("should execute: [{Hello:'World'}].toJsonPretty()") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """[{Hello:'World'}].toJsonPretty()""")
      assert(results ==
        """|[{
           |  "Hello": "World"
           |}]""".stripMargin)
    }
  }

  describe("Any:toPrettyString()") {
    it("should execute the 'toPrettyString()' function") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), "'Hello'.getBytes().toPrettyString()")
      assert(results == "Hello")
    }
  }

  describe("Any:toTable()") {
    it("should convert a Row into a table") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|val rows = (
           |   |------------------------------|
           |   | symbol | exchange | lastSale |
           |   |------------------------------|
           |   | XYZ    | AMEX     |    31.95 |
           |   | AAXX   | NYSE     |    56.12 |
           |   | QED    | NASDAQ   |          |
           |   | JUNK   | AMEX     |    97.61 |
           |   |------------------------------|
           |)
           |rows[0]//.toTable()
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95)
      ))
    }

    it("should convert an array of dictionaries into a table") {
      val (_, _, results) = QweryVM.searchSQL(Scope(),
        """|[{
           |  symbol: 'ABC',
           |  exchange: 'NYSE',
           |  lastSale: 56.98,
           |  lastSaleTime: DateTime(1631508164812),
           |  codes: ["ABC"]
           |},{
           |  symbol: 'GE',
           |  exchange: 'NASDAQ',
           |  lastSale: 83.13,
           |  lastSaleTime: DateTime(1631508164812),
           |  codes: ["XYZ"]
           |},{
           |  symbol: 'GMTQ',
           |  exchange: 'OTCBB',
           |  lastSale: 0.1111,
           |  lastSaleTime: DateTime(1631508164812),
           |  codes: ["ABC", "XYZ"]
           |}].toTable()
           |""".stripMargin
      )
      assert(results.tabulate() ==
        """||--------------------------------------------------------------------------|
           || exchange | symbol | codes          | lastSale | lastSaleTime             |
           ||--------------------------------------------------------------------------|
           || NYSE     | ABC    | ["ABC"]        |    56.98 | 2021-09-13T04:42:44.812Z |
           || NASDAQ   | GE     | ["XYZ"]        |    83.13 | 2021-09-13T04:42:44.812Z |
           || OTCBB    | GMTQ   | ["ABC", "XYZ"] |   0.1111 | 2021-09-13T04:42:44.812Z |
           ||--------------------------------------------------------------------------|
           |""".stripMargin.trim.split("\n").toList)
    }

    it("should verify the types of an array of dictionaries converted into a table") {
      val (_, _, results) = QweryVM.searchSQL(Scope(),
        """|describe([{
           |  symbol: 'ABC',
           |  exchange: 'NYSE',
           |  lastSale: 56.98,
           |  lastSaleTime: DateTime(1631508164812),
           |  codes: ["ABC"]
           |},{
           |  symbol: 'GE',
           |  exchange: 'NASDAQ',
           |  lastSale: 83.13,
           |  lastSaleTime: DateTime(1631508164812),
           |  codes: ["XYZ"]
           |},{
           |  symbol: 'GMTQ',
           |  exchange: 'OTCBB',
           |  lastSale: 0.1111,
           |  lastSaleTime: DateTime(1631508164812),
           |  codes: ["ABC", "XYZ"]
           |}].toTable())
           |""".stripMargin
      )
      assert(results.toMapGraph == List(
        Map("name" -> "exchange", "type" -> "String(6)"),
        Map("name" -> "symbol", "type" -> "String(4)"),
        Map("name" -> "codes", "type" -> "String(256)[2]"),
        Map("name" -> "lastSale", "type" -> "Double"),
        Map("name" -> "lastSaleTime", "type" -> "DateTime")
      ))
    }
  }

  describe("Array:contains()") {
    it("should execute: [1, 3, 5, 7, 9].contains(5)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), """[1, 3, 5, 7, 9].contains(5)""")
      assert(result == true)
    }
    it("should decompile: [1, 3, 5, 7, 9].contains(5)")(decompile("[1, 3, 5, 7, 9].contains(5)"))
  }

  describe("Array:distinctValues()") {
    it("should execute: [1, 3, 5, 7, 9, 7, 3, 2, 1].distinctValues()") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """[1, 3, 5, 7, 9, 7, 3, 2, 1].distinctValues()""")
      assert(toList(results) == List(1, 3, 5, 7, 9, 2))
    }

    it("should execute: [1, 3, 5, 7, 9, 7, 3, 2, 1, null].distinctValues()") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """[1, 3, 5, 7, 9, 7, 3, 2, 1, null].distinctValues()""")
      assert(toList(results) == List(1, 3, 5, 7, 9, 2, null))
    }

    it("should execute: [1, 3, 5, 7].foldLeft(0, (a: Int, b: Int) => a + b)") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """[1, 3, 5, 7].foldLeft(0, (a: Int, b: Int) => a + b)""")
      assert(results == 16)
    }
  }

  describe("Array:filter()") {
    it("should execute: [1 to 10].filter((n: Int) => (n % 2) == 0)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """[1 to 10].filter((n: Int) => (n % 2) == 0)""")
      assert(Option(result).collect { case a: Array[_] => a.toList } contains List(2, 4, 6, 8, 10))
    }
  }

  describe("Array:foreach()") {
    it("should execute: [1, 3, 5, 7, 9].foreach( (n: Int) => out.println(n) )") {
      val (scope, _, _) = QweryVM.executeSQL(ctx.createRootScope(),
        """[1, 3, 5, 7, 9].foreach( (n: Int) => out.println(n) )""")
      assert(scope.getUniverse.system.stdOut.asString() ==
        """|1
           |3
           |5
           |7
           |9
           |""".stripMargin)

    }

    it("should execute: [x to y].foreach(...)") {
      val (scope, _, _) = QweryVM.executeSQL(Scope(),
        """|set x = 0, y = 4
           |[x to y].foreach((n: Int) => x += n )
           |x
           |""".stripMargin)
      assert(scope.resolve("x") contains 10)
    }

    it("should execute: [x until y].foreach(...)") {
      val (scope, _, _) = QweryVM.executeSQL(Scope(),
        """|set x = 0, y = 4
           |[x until y].foreach((n: Int) => x += n )
           |x
           |""".stripMargin)
      assert(scope.resolve("x") contains 6)
    }

    it("should execute: 1 to 5].foreach(...)") {
      val (_, _, result) = QweryVM.searchSQL(Scope(),
        """|declare table myQuotes(symbol: String(4), exchange: String(6), lastSale: Float, lastSaleTime: DateTime)
           |[1 to 5].foreach((n: Int) => {
           |  insert into @@myQuotes (lastSaleTime, lastSale, exchange, symbol)
           |  select
           |    lastSaleTime: DateTime(),
           |    lastSale: scaleTo(500 * Random.nextDouble(0.99), 4),
           |    exchange: ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB'][Random.nextInt(4)],
           |    symbol: Random.nextString(['A' to 'Z'], 4)
           |})
           |return @@myQuotes
           |""".stripMargin)
      assert(result.getLength == 5)
    }
  }

  describe("Array:indexOf()") {
    it("""should execute: ['A' to 'Z'].indexOf('C')""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), """['A' to 'Z'].indexOf('C')""")
      assert(result == 2)
    }
  }

  describe("Array:isEmpty()") {
    it("should execute: [].isEmpty()")(isTrue("[].isEmpty()"))
    it("should execute: [1, 2, 3].isEmpty()")(isFalse("[1, 2, 3].isEmpty()"))
  }

  describe("Array:join()") {
    it("""should execute: ['A', 'p', 'p', 'l', 'e', '!'].join("")""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), """['A', 'p', 'p', 'l', 'e', '!'].join("")""")
      assert(result == "Apple!")
    }

    it("""should execute: ['A', 'p', 'p', 'l', 'e', '!'].join('|')""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), """['A', 'p', 'p', 'l', 'e', '!'].join('|')""")
      assert(result == "A|p|p|l|e|!")
    }

    it("should execute: [1, 3, 5, 7, 9].join(', ')") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), """[1, 3, 5, 7, 9].join(', ')""")
      assert(result == "1, 3, 5, 7, 9")
    }
  }

  describe("Array:length()") {
    it("should execute: [1 to 5].length()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), """[1 to 5].length()""")
      assert(result == 5)
    }
  }

  describe("Array:map()") {
    it("should execute: [1 to 5].map((n: int) => n * n)") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """[1 to 5].map((n: int) => n * n)""")
      assert(toList(results) == List(1.0, 4.0, 9.0, 16.0, 25.0))
    }
  }

  describe("Array:maxValue()") {
    it("should execute: [1, 3, 5, 7, 9].maxValue()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "[1, 3, 5, 7, 9].maxValue()")
      assert(result == 9)
    }

    it("should execute: [1, 13, 3, 5, null, 7, 9].maxValue()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "[1, 13, 3, 5, null, 7, 9].maxValue()")
      assert(result == 13)
    }
  }

  describe("Array:minValue()") {
    it("should execute: [1, 3, 5, 7, 9].minValue()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "[1, 3, 5, 7, 9].minValue()")
      assert(result == 1)
    }

    it("should execute: [1, 3, 5, 0, null, 7, 9].minValue()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "[1, 3, 5, 0, null, 7, 9].minValue()")
      assert(result == 0)
    }
  }

  describe("Array:nonEmpty()") {
    it("should execute: [].nonEmpty()")(isFalse("[].nonEmpty()"))
    it("should execute: [1, 2, 3].nonEmpty()")(isTrue("[1, 2, 3].nonEmpty()"))
  }

  describe("Array:push()") {
    it("should execute: ['A' to 'D'].push('Z')") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """['A' to 'D'].push('Z')""")
      assert(toList(results) == List('A', 'B', 'C', 'D', 'Z'))
    }

    it("should execute: ['A' to 'C'].push(['X', 'Y', 'Z'])") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """['A' to 'C'].push(['X', 'Y', 'Z'])""")
      assert(toList(results) == List('A', 'B', 'C', 'X', 'Y', 'Z'))
    }
  }

  describe("Array:reverse()") {
    it("should execute: array.map((arr: double[3]) => arr.reverse())") {
      val (scope, _, _) = QweryVM.executeSQL(Scope(),
        """|set array = [[1 to 3], [2 to 4], [4, 5, 6]]
           |set items = array.map((arr: Double[]) => arr.reverse()).toPrettyString()
           |""".stripMargin
      )
      val result = scope.resolve("items")
      assert(result contains "[[3, 2, 1], [4, 3, 2], [6, 5, 4]]")
    }

    it("""should execute: ['A' to 'Z'].reverse()""") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """['A' to 'Z'].reverse()""")
      assert(toList(results) == ('A' to 'Z').toList.reverse)
    }
  }

  describe("Array:slice()") {
    it("should execute: ['A' to 'Z'].slice(1, 5)") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """['A' to 'Z'].slice(1, 5)""")
      assert(toList(results) == List('B', 'C', 'D', 'E'))
    }
  }

  describe("Array:sortValues()") {
    it("should execute: [3, 9, 5, 2, 7, 1].sortValues()") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """[3, 9, 5, 2, 7, 1].sortValues()""")
      assert(toList(results) == List(1, 2, 3, 5, 7, 9))
    }

    it("should execute: [3, 9, 5, null, 2, 7, 1].sortValues()") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """[3, 9, 5, null, 2, 7, 1].sortValues()""")
      assert(toList(results) == List(1, 2, 3, 5, 7, 9, null))
    }
  }

  describe("Bytes:base64(), unBase64()") {
    it("""should execute: "Hello".getBytes().base64()""") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """String("Hello".getBytes().base64().unBase64())""")
      assert(results == "Hello")
    }
  }

  describe("Bytes:fromHex(), toHex()") {
    it("""should execute: "deadbeef".fromHex().toHex()""") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """"deadbeef".fromHex().toHex()""")
      assert(results == "deadbeef")
    }
  }

  describe("Bytes:gzip(), gunzip()") {
    it("""should execute: "Hello".getBytes().gzip()""") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """String("Hello".getBytes().gzip().gunzip())""")
      assert(results == "Hello")
    }
  }

  describe("Bytes:md5(), toHex()") {
    it("""should execute: "Hello".getBytes().md5()""") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """String("Hello".getBytes().md5().toHex())""")
      assert(results == "8b1a9953c4611296a827abf8c47804d7")
    }
  }

  describe("Bytes:serialize(), deserialize()") {
    it("""should execute: "What did the fox say?".serialize().deserialize()""") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """"What did the fox say?".serialize().deserialize()""")
      assert(results == "What did the fox say?")
    }
  }

  describe("Bytes:snappy(), unSnappy()") {
    it("""should execute: "Hello".getBytes().snappy()""") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), """String("Hello".getBytes().snappy().unSnappy())""")
      assert(results == "Hello")
    }
  }

  describe("Date:dayOfMonth()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').dayOfMonth()") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z').dayOfMonth()")
      assert(results == 2)
    }
  }

  describe("Date:dayOfWeek()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').dayOfWeek()") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z').dayOfWeek()")
      assert(results == 5)
    }
  }

  describe("Date:format()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').format('yyyy-MM-dd')") {
      val (_, _, results) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z').format('yyyy-MM-dd')")
      assert(results == "2021-09-02")
    }
  }

  describe("Date:hour()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').hour()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z').hour()")
      assert(result == 11)
    }
  }

  describe("Date:millisecond()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').millisecond()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.375Z').millisecond()")
      assert(result == 375)
    }
  }

  describe("Date:minute()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').minute()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z').minute()")
      assert(result == 22)
    }
  }

  describe("Date:month()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').month()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z').month()")
      assert(result == 9)
    }
  }

  describe("Date:second()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').second()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z').second()")
      assert(result == 33)
    }
  }

  describe("Date:split()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').split()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.357Z').split()")
      val combined = Option(result).collect { case (a: LocalDate, b: LocalTime) => a + "T" + b }
      assert(combined contains "2021-09-02T11:22:33.357")
    }
  }

  describe("Date:toHttpDate()") {
    it("should evaluate: DateTime('2022-09-04T23:36:47.846Z').toHttpDate()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|DateTime('2022-09-04T23:36:47.846Z').toHttpDate()
           |""".stripMargin)
      assert(result == "Sun, 04 Sep 2022 23:36:47 GMT")
    }
  }

  describe("Date:year()") {
    it("should execute: DateTime('2021-09-02T11:22:33.000Z').year()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "DateTime('2021-09-02T11:22:33.000Z').year()")
      assert(result == 2021)
    }
  }

  describe("Numeric:evaluate()") {
    it("""should execute: 23.asBinaryString()""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), "23.asBinaryString()")
      assert(result == "10111")
    }
  }

  describe("String:fromJson()") {
    it("""should execute: '{ "name": "height", "value": 76 }'.fromJson()""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), """'{ "name": "height", "value": 76 }'.fromJson()""")
      assert(result == Map("name" -> "height", "value" -> 76))
    }

    it("""should execute: ('{ "symbol": "ABC", "exchange": "NYSE" }'.fromJson()).symbol""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), """('{ "symbol": "ABC", "exchange": "NYSE" }'.fromJson()).symbol""")
      assert(result == "ABC")
    }
  }

  describe("String:reverse()") {
    it("""should execute: "Hello World".reverse()""") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), """"Hello World".reverse()""")
      assert(result == "dlroW olleH")
    }
  }

  describe("String:stripMargin()") {
    it("supports parsing multi-line singled-quoted strings with a margin") {
      val (_, _, result) = QweryVM.executeSQL(Scope(), sql =
        """''';var count = 0
              ;while(count < 5) {
              ;  println(s"Hello $count")
              ;  count += 1
              ;}
              ;count
              ;'''.stripMargin(';')""")
      assert(result ==
        """|var count = 0
           |while(count < 5) {
           |  println(s"Hello $count")
           |  count += 1
           |}
           |count
           |""".stripMargin)
    }
  }

  private def decompile(sql: String): Assertion = {
    val model = QweryCompiler().compile(sql)
    assert(model.toSQL == sql)
  }

  private def isTrue(sql: String): Assertion = {
    val result = QweryVM.executeSQL(Scope(), sql)._3
    assert(result == true)
  }

  private def isFalse(sql: String): Assertion = {
    val result = QweryVM.executeSQL(Scope(), sql)._3
    assert(result == false)
  }

  private def toList(results: Any): List[Any] = {
    safeCast[Array[_]](results).toList.flatMap(_.toList)
  }

}
