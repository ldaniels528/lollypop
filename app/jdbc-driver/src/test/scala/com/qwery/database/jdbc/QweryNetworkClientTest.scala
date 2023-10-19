package com.qwery.database.jdbc

import com.qwery.AppConstants.{DEFAULT_DATABASE, DEFAULT_SCHEMA}
import com.qwery.runtime.QweryCompiler
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import java.io.File

class QweryNetworkClientTest extends AnyFunSpec with JDBCTestServer {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[QweryNetworkClient].getSimpleName) {

    it("should generate the JDBC URL") {
      val url = QweryNetworkClient.createURL(host = "0.0.0.0", port = 8233)
      assert(url == s"jdbc:qwery://0.0.0.0:8233/$DEFAULT_DATABASE.$DEFAULT_SCHEMA")
    }

    it("should execute inline scripts") {
      QweryNetworkClient.getConnection(classOf[QweryDriver].getName, jdbcURL) use { conn =>
        QweryNetworkClient.interact(conn, console = {
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
      QweryNetworkClient.getConnection(classOf[QweryDriver].getName, jdbcURL) use { implicit conn =>
        val outputFile = new File("./vin-mapping.json")
        assert(!outputFile.exists() || outputFile.delete())
        QweryNetworkClient.startScript(new File("./contrib/examples/src/main/qwery/GenerateVinMapping.sql"))
        assert(outputFile.exists())
      }
    }

  }

}
