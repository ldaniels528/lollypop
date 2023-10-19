package com.qwery.database.jdbc

import com.qwery.language.models.Expression.implicits.LifestyleExpressions
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.{BlobType, ClobType, SQLXMLType}
import com.qwery.runtime.instructions.VerificationTools
import org.scalatest.funspec.AnyFunSpec

class JDBCUniverseTest extends AnyFunSpec with JDBCTestServer with VerificationTools {

  describe(classOf[JDBCUniverse.type].getSimpleName) {

    it("should resolve BLOB type") {
      implicit val scope: Scope = Scope()
      val u = JDBCUniverse()
      assert(u.dataTypeParsers.flatMap(_.parseDataType("BLOB".ct)) contains BlobType)
    }

    it("should resolve CLOB type") {
      implicit val scope: Scope = Scope()
      val u = JDBCUniverse()
      assert(u.dataTypeParsers.flatMap(_.parseDataType("CLOB".ct)) contains ClobType)
    }

    it("should resolve SQLXML type") {
      implicit val scope: Scope = Scope()
      val u = JDBCUniverse()
      assert(u.dataTypeParsers.flatMap(_.parseDataType("SQLXML".ct)) contains SQLXMLType)
    }

    it("should resolve XML type") {
      implicit val scope: Scope = Scope()
      val u = JDBCUniverse()
      assert(u.dataTypeParsers.flatMap(_.parseDataType("XML".ct)) contains SQLXMLType)
    }

  }

}
