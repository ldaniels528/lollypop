package qwery.lang

import com.qwery.AppConstants
import com.qwery.database.QueryResponse
import com.qwery.language.QweryUniverse
import com.qwery.runtime.RuntimeFiles.RecursiveFileList
import com.qwery.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.JSONSupport.JSONStringConversion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory
import spray.json.enrichAny

import java.io.File
import scala.util.Try

class OSTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[OS].getSimpleName) {

    it("should change the current working directory") {
      val (scope, _, _) = QweryVM.executeSQL(Scope(),
        """|OS.chdir('./contrib/examples')
           |""".stripMargin)
      assert(scope.getUniverse.system.currentDirectory contains new File("./contrib/examples"))
    }

    it("""should compile and execute: "select 'Hello World' as message".evaluate()""") {
      val (_, _, result) = QweryVM.searchSQL(Scope(),
        """|val a = "select 'Hello World'"
           |val b = "message"
           |val code = OS.compile(a + " as " + b)
           |OS.run(code)
           |""".stripMargin)
      assert(result.toMapGraph == List(Map("message" -> "Hello World")))
    }

    it("should execute: OS.getEnv()") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|from OS.getEnv() where name is 'HOME'
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("name" -> "HOME", "value" -> scala.util.Properties.userHome),
      ))
    }

    it("should execute: OS.exec('whoami')") {
      Try {
        val (_, _, device) = QweryVM.searchSQL(Scope(),
          """|OS.exec('whoami')
             |""".stripMargin)
        device.tabulate() foreach logger.info
      }
    }

    it("should execute: OS.listFiles('/examples/src/main/qwery')") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|from OS.listFiles('./contrib/examples/src/main/qwery') where name like '%.sql'
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph.collect { case m: Map[String, Any] => m("name") }.toSet == Set(
        "BlackJack.sql", "GenerateVinMapping.sql", "SwingDemo.sql", "MacroDemo.sql", "IngestDemo.sql",
        "BreakOutDemo.sql", "Stocks.sql"
      ))
    }

    it("should execute: OS.listFiles('/examples/src/main/qwery', true)") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|from OS.listFiles('./contrib/examples/src/', true) where name like '%.sql'
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph.collect { case m: Map[String, Any] => m("name") }.toSet == Set(
        "Contests.sql", "position_api.sql", "stock_api.sql", "StockQuotes.sql", "order_api.sql", "Participants.sql",
        "commons.sql", "Orders.sql", "Members.sql", "MacroDemo.sql", "member_api.sql", "BlackJack.sql",
        "GenerateVinMapping.sql", "Positions.sql", "SwingDemo.sql", "participant_api.sql", "shocktrade.sql",
        "shocktrade_test.sql", "contest_api.sql", "IngestDemo.sql", "BreakOutDemo.sql", "Stocks.sql"
      ))
    }

    it("should execute: OS.read('README.md')") {
      val (_, _, results) = QweryVM.searchSQL(Scope(), "OS.read('README.md')")
      results.tabulate(5) foreach logger.info
      assert(results.tabulate(5).mkString("\n") ==
        s"""||--------------------------------------------|
            || line                                       |
            ||--------------------------------------------|
            || Qwery v${AppConstants.version}                               |
            || ============                               |
            ||                                            |
            || ## Table of Contents                       |
            || * <a href="#Introduction">Introduction</a> |
            ||--------------------------------------------|
            |""".stripMargin.trim)
    }

    it("should execute: OS.sizeOf(BitArray(3, 6, 9))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(BitArray(3, 6, 9))
           |""".stripMargin)
      assert(value == 20)
    }

    it("should execute: OS.sizeOf(Byte(6))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(Byte(6))
           |""".stripMargin)
      assert(value == 1)
    }

    it("should execute: OS.sizeOf(ByteBuffer.allocate(32))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|import 'java.nio.ByteBuffer'
           |OS.sizeOf(ByteBuffer.allocate(32))
           |""".stripMargin)
      assert(value == 32)
    }

    it("should execute: OS.sizeOf(Char('z'))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(Char('z'))
           |""".stripMargin)
      assert(value == 2)
    }

    it("should execute: OS.sizeOf(DateTime('2023-05-02T11:22:33.000Z'))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(DateTime('2023-05-02T11:22:33.000Z'))
           |""".stripMargin)
      assert(value == 8)
    }

    it("should execute: OS.sizeOf(Double(5.8))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(Double(5.8))
           |""".stripMargin)
      assert(value == 8)
    }

    it("should execute: OS.sizeOf(new File('./build.sbt'))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|import 'java.io.File'
           |OS.sizeOf(new File('./build.sbt'))
           |""".stripMargin)
      assert(value == new File("./build.sbt").length())
    }

    it("should execute: OS.sizeOf(Float(5.8))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(Float(5.8))
           |""".stripMargin)
      assert(value == 4)
    }

    it("should execute: OS.sizeOf(Int(100))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(Int(100))
           |""".stripMargin)
      assert(value == 4)
    }

    it("should execute: OS.sizeOf(Long(100))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(Long(100))
           |""".stripMargin)
      assert(value == 8)
    }

    it("should execute: OS.sizeOf(Short(100))") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(Short(100))
           |""".stripMargin)
      assert(value == 2)
    }

    it("should execute: OS.sizeOf(1.0)") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(1.0)
           |""".stripMargin)
      assert(value == 8)
    }

    it("should execute: OS.sizeOf(DateTime())") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf(DateTime())
           |""".stripMargin)
      assert(value == 8)
    }

    it("should execute: OS.sizeOf('Hello World')") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf('Hello World')
           |""".stripMargin)
      assert(value == 11)
    }

    it("should execute: OS.sizeOf(stocks)") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|val stocks = TableZoo(symbol: String(7), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)
           |  .withMemorySupport(150)
           |  .build()
           |OS.sizeOf(stocks)
           |""".stripMargin)
      assert(value == 6300)
    }

    it("should execute: OS.sizeOf([{price:35.11, ... ])") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|OS.sizeOf([
           |   {"price":35.11, "transactionTime":"2021-08-05T19:23:12.000Z"},
           |   {"price":35.83, "transactionTime":"2021-08-05T19:23:15.000Z"},
           |   {"price":36.03, "transactionTime":"2021-08-05T19:23:17.000Z"}
           |])
           |""".stripMargin)
      assert(value == 156)
    }

    it("should recursively copy a directory structure") {
      // recursive copy a directory structure
      val (scopeA, _, resultA) = QweryVM.executeSQL(Scope(),
        """|OS.copy("./contrib/examples/", "temp_demos/")
           |""".stripMargin)
      val (srcDir, destDir) = (new File("./contrib/examples/"), new File("temp_demos/"))
      assert(resultA == true & srcDir.streamFilesRecursively.map(_.getName) == destDir.streamFilesRecursively.map(_.getName))

      // recursive delete the directory structure
      val (_, _, resultB) = QweryVM.executeSQL(scopeA,
        """|OS.delete("temp_demos/", true)
           |""".stripMargin)
      assert(resultB == true & !destDir.exists())
    }

    it("should capture and return the output of STDERR on the server-side") {
      val scope0 = QweryUniverse(isServerMode = true).createRootScope()
      val (scope1, _, _) = QweryVM.executeSQL(scope0,
        """|stderr.println("1. Always be respective")
           |stderr.println("2. Always be honest")
           |stderr.println("3. Always be kind")
           |""".stripMargin)
      assert(scope1.getUniverse.system.stdErr.asString() ==
        """|1. Always be respective
           |2. Always be honest
           |3. Always be kind
           |""".stripMargin)
    }

    it("should capture and return the output of STDOUT on the server-side") {
      val scope0 = QweryUniverse(isServerMode = true).createRootScope()
      val (scope1, _, _) = QweryVM.executeSQL(scope0,
        """|stdout <=== "1. Always be respective\n"
           |stdout <=== "2. Always be honest\n"
           |stdout <=== "3. Always be kind\n"
           |""".stripMargin)
      assert(scope1.getUniverse.system.stdOut.asString() ==
        """|1. Always be respective
           |2. Always be honest
           |3. Always be kind
           |""".stripMargin)
    }

    it("should simulate input/output of STDERR on the server-side") {
      val ctx = QweryUniverse(isServerMode = true)
      ctx.system.stdErr.writer.println("Hello World")
      val message = ctx.system.stdErr.reader.readLine()
      assert(message == "Hello World")
    }

    it("should simulate input/output of STDIN on the server-side") {
      val ctx = QweryUniverse(isServerMode = true)
      ctx.system.stdIn.writer.println("Hello World")
      val message = ctx.system.stdIn.reader.readLine()
      assert(message == "Hello World")
    }

    it("should simulate input/output of STDOUT on the server-side") {
      val ctx = QweryUniverse(isServerMode = true)
      ctx.system.stdOut.writer.println("Hello World")
      val message = ctx.system.stdOut.reader.readLine()
      assert(message == "Hello World")
    }

    it("should return a result as a QueryResponse") {
      val (_, _, outcome) = QweryVM.executeSQL(Scope(),
        """|OS.execQL("select symbol: 'GMTQ', exchange: 'OTCBB', lastSale: 0.1111")
           |""".stripMargin)
      val results = outcome match {
        case q: QueryResponse => q.toRowCollection.toMapGraph
        case _ => Nil
      }
      assert(results == List(Map("symbol" -> "GMTQ", "exchange" -> "OTCBB", "lastSale" -> 0.1111)))
    }

    it("should return a result as a JSON object graph") {
      implicit val scope: Scope = Scope()
      val response = OS.execQL("select symbol: 'GMTQ', exchange: 'OTCBB', lastSale: 0.1111")
      assert(response.toJson ==
        """|{
           |  "columns": [
           |    {
           |      "name": "symbol",
           |      "type": "String(4)"
           |    },
           |    {
           |      "name": "exchange",
           |      "type": "String(5)"
           |    },
           |    {
           |      "name": "lastSale",
           |      "type": "Double"
           |    }
           |  ],
           |  "cost": {
           |    "shuffled": 0,
           |    "rowIdStart": 0,
           |    "matched": 0,
           |    "updated": 0,
           |    "destroyed": 0,
           |    "scanned": 0,
           |    "inserted": 0,
           |    "altered": 0,
           |    "deleted": 0,
           |    "created": 0
           |  },
           |  "ns": "qwery.public.???",
           |  "resultType": "ROWS",
           |  "rows": [
           |    [
           |      "GMTQ",
           |      "OTCBB",
           |      0.1111
           |    ]
           |  ],
           |  "stdErr": "",
           |  "stdOut": ""
           |}
           |""".stripMargin.parseJSON)
    }

  }

}