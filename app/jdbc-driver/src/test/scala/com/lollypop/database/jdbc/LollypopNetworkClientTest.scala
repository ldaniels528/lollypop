package com.lollypop.database.jdbc

import com.lollypop.AppConstants.{DEFAULT_DATABASE, DEFAULT_SCHEMA}
import com.lollypop.runtime.LollypopCompiler
import com.lollypop.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class LollypopNetworkClientTest extends AnyFunSpec with JDBCTestServer {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[LollypopNetworkClient].getSimpleName) {

    it("should generate the JDBC URL") {
      val url = LollypopNetworkClient.createURL(host = "0.0.0.0", port = 8233)
      assert(url == s"jdbc:lollypop://0.0.0.0:8233/$DEFAULT_DATABASE.$DEFAULT_SCHEMA")
    }

    it("should execute inline scripts") {
      LollypopNetworkClient.getConnection(classOf[LollypopDriver].getName, jdbcURL) use { conn =>
        LollypopNetworkClient.interact(conn, console = {
          val it = Seq(
            "val x = 3",
            "val y = 6",
            "val z = x + y"
          ).iterator
          () => if (it.hasNext) it.next() else "exit"
        })
      }
    }

    it("should execute a .sql file via runScript(...)") {
      LollypopNetworkClient.getConnection(classOf[LollypopDriver].getName, jdbcURL) use { implicit conn =>
        val outputFile = new File("./vin-mapping.json")
        assert(!outputFile.exists() || outputFile.delete())
        LollypopNetworkClient.startScript(new File("./contrib/examples/src/main/lollypop/GenerateVinMapping.sql"))
        assert(outputFile.exists())
      }
    }

  }

}
