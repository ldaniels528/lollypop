package com.lollypop.runtime

import com.lollypop.language.models.{Column, ColumnType}
import com.lollypop.runtime.ModelsJsonProtocol._
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.TableColumn
import com.lollypop.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import spray.json._

/**
 * Models JSON Protocol Test
 */
class ModelsJsonProtocolTest extends AnyFunSpec {

  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(ModelsJsonProtocol.getClass.getSimpleName) {

    it("should serialize/deserialize a Column") {
      verifyJson(Column(name = "symbol", `type` = ColumnType(name = "string", size = 24)))
    }

    it("should serialize/deserialize a DataType") {
      implicit val scope: Scope = Scope()
      val types = Seq[DataType](
        ArrayType(componentType = Int8Type, capacity = Some(5)), BlobType, BlobType, BooleanType, CharType, ClobType,
        Float64Type, EnumType(Seq("a", "b", "c")), Float32Type, Int32Type, Int64Type, Int16Type, NumericType, AnyType,
        TableType(columns = Seq(
          Column(name = "remarks", `type` = ColumnType("String", size = StringType.maxSizeInBytes)),
          Column(name = "createdDate", `type` = ColumnType("DateTime"))
        ).map(_.toTableColumn), capacity = 5), UUIDType
      )
      types.foreach(verifyJson(_))
    }

    it("should serialize/deserialize a DatabaseObjectRef") {
      assert(DatabaseObjectRef("shocktrade.portfolios.stocks") == DatabaseObjectNS(databaseName = "shocktrade", schemaName = "portfolios", name = "stocks"))
      verifyJson(DatabaseObjectRef("shocktrade.portfolios.stocks"))
    }

    it("should serialize/deserialize a TableColumn") {
      verifyJson(TableColumn(name = "symbol", `type` = StringType(24)))
    }

    it("should serialize/deserialize a TableConfig") {
      verifyJson(DatabaseObjectConfig(description = Some("test config"), columns = Seq(
        TableColumn(name = "symbol", `type` = StringType(24))
      )))
    }

  }

  def verifyJson[A](entity0: A)(implicit reader: JsonReader[A], writer: JsonWriter[A]): Assertion = {
    val entity0Js = entity0.toJSON
    info(entity0Js)
    val entity1 = entity0Js.fromJSON[A]
    assert(entity0 == entity1)
  }

}
