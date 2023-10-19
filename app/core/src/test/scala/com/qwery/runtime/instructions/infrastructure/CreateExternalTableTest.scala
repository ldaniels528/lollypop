package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.ExternalTable
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.{ArrayLiteral, Dictionary}
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler}
import org.scalatest.funspec.AnyFunSpec

class CreateExternalTableTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[CreateExternalTable].getSimpleName) {

    it("should compile create external table") {
      val results = compiler.compile(
        """|create external table Customers (customer_uid: UUID, name: String, address: String, ingestion_date: Long)
           |containing {
           |  partitions: ['year', 'month', 'day'],
           |  format: 'JSON',
           |  location: './dataSets/customers/json/'
           |}
           |""".stripMargin)
      assert(results == CreateExternalTable(ref = DatabaseObjectRef("Customers"),
        ExternalTable(
          columns = List("customer_uid UUID".c, "name String".c, "address String".c, "ingestion_date Long".c),
          options = Dictionary(
            "partitions" -> ArrayLiteral("year".v, "month".v, "day".v),
            "format" -> "JSON".v,
            "location" -> "./dataSets/customers/json/".v
          )
        ), ifNotExists = false))
    }

    it("should decompile create external table") {
      val model = CreateExternalTable(ref = DatabaseObjectRef("Customers"),
        ExternalTable(
          columns = List("customer_uid UUID".c, "name String".c, "address String".c, "ingestion_date Long".c),
          options = Dictionary(
            "partitions" -> ArrayLiteral("year".v, "month".v, "day".v),
            "format" -> "JSON".v,
            "location" -> "./dataSets/customers/json/".v
          )
        ), ifNotExists = false)
      assert(model.toSQL ==
        """|create external table Customers (customer_uid: UUID, name: String, address: String, ingestion_date: Long)
           |containing { partitions: ["year", "month", "day"], format: "JSON", location: "./dataSets/customers/json/" }
           |""".stripMargin.trim.replaceAll("\n", " "))
    }

    it("should compile create external table w/comment") {
      val results = compiler.compile(
        """|create external table Customers (
           |    customer_uid UUID,
           |    name String,
           |    address String,
           |    ingestion_date Long
           |) containing {
           |   partitions: ['year', 'month', 'day'],
           |   field_delimiter: ",",
           |   format: 'CSV',
           |   headers: true,
           |   null_values: ['n/a'],
           |   location: './dataSets/customers/csv/'
           |}
           |""".stripMargin)
      assert(results == CreateExternalTable(ref = DatabaseObjectRef("Customers"), ExternalTable(
        columns = List("customer_uid UUID".c, "name String".c, "address String".c, "ingestion_date Long".c),
        options = Dictionary(
          "partitions" -> ArrayLiteral("year".v, "month".v, "day".v),
          "field_delimiter" -> ",",
          "format" -> "CSV",
          "headers" -> true,
          "null_values" -> ArrayLiteral("n/a"),
          "location" -> "./dataSets/customers/csv/"
        )
      ), ifNotExists = false))
    }

    it("should decompile create external table w/comment") {
      val model = CreateExternalTable(ref = DatabaseObjectRef("Customers"), ExternalTable(
        columns = List("customer_uid UUID".c, "name String".c, "address String".c, "ingestion_date Long".c),
        options = Dictionary(
          "partitions" -> ArrayLiteral("year".v, "month".v, "day".v),
          "field_delimiter" -> ",",
          "headers" -> true,
          "null_values" -> ArrayLiteral("n/a"),
          "format" -> "CSV",
          "location" -> "./dataSets/customers/csv/"
        )
      ), ifNotExists = false)
      assert(model.toSQL ==
        """|create external table Customers (customer_uid: UUID, name: String, address: String, ingestion_date: Long)
           |containing { partitions: ["year", "month", "day"], field_delimiter: ",", headers: true, null_values: ["n/a"], format: "CSV", location: "./dataSets/customers/csv/" }
           |""".stripMargin.trim.replaceAll("\n", " "))
    }

    it("should compile create external table .. WITH") {
      val results = compiler.compile(
        """|create external table Customers (customer_uid: UUID, name: String, address: String, ingestion_date: Long)
           |containing {
           |   partitions: ['year', 'month', 'day'],
           |   field_delimiter: ",",
           |   format: 'CSV',
           |   headers: true,
           |   null_values: ['n/a'],
           |   location: './dataSets/customers/csv/'
           |}
           |""".stripMargin)
      assert(results == CreateExternalTable(ref = DatabaseObjectRef("Customers"),
        ExternalTable(
          columns = List("customer_uid UUID".c, "name String".c, "address String".c, "ingestion_date Long".c),
          options = Dictionary(
            "partitions" -> ArrayLiteral("year".v, "month".v, "day".v),
            "field_delimiter" -> ",",
            "format" -> "CSV",
            "headers" -> true,
            "null_values" -> ArrayLiteral("n/a"),
            "location" -> "./dataSets/customers/csv/"
          )
        ), ifNotExists = false))
    }

    it("should decompile create external table .. WITH") {
      val model = CreateExternalTable(ref = DatabaseObjectRef("Customers"),
        ExternalTable(
          columns = List("customer_uid UUID".c, "name String".c, "address String".c, "ingestion_date Long".c),
          options = Dictionary(
            "partitions" -> ArrayLiteral("year".v, "month".v, "day".v),
            "field_delimiter" -> ",",
            "headers" -> true,
            "null_values" -> ArrayLiteral("n/a"),
            "format" -> "CSV",
            "location" -> "./dataSets/customers/csv/"
          )
        ), ifNotExists = false)
      assert(model.toSQL ==
        """|create external table Customers (customer_uid: UUID, name: String, address: String, ingestion_date: Long)
           |containing { partitions: ["year", "month", "day"], field_delimiter: ",", headers: true, null_values: ["n/a"], format: "CSV", location: "./dataSets/customers/csv/" }
           |""".stripMargin.trim.replaceAll("\n", " "))
    }

    it("should compile create external table .. partitions") {
      val results = compiler.compile(
        """|create external table revenue_per_page (
           |  `rank` String,
           |  `section` String,
           |  `super_section` String,
           |  `page_name` String,
           |  `rev` String,
           |  `last_processed_ts_est` String)
           |containing {
           |   partitions: ['hit_dt_est', 'site_experience_desc'],
           |   format: 'CSV',
           |   location: '/web/revenue_per_page/'
           |}
           |""".stripMargin)
      assert(results == CreateExternalTable(ref = DatabaseObjectRef("revenue_per_page"),
        ExternalTable(
          columns = List(
            "rank String".c, "section String".c, "super_section String".c, "page_name String".c,
            "rev String".c, "last_processed_ts_est String".c
          ),
          options = Dictionary(
            "partitions" -> ArrayLiteral("hit_dt_est".v, "site_experience_desc".v),
            "format" -> "CSV",
            "location" -> "/web/revenue_per_page/"
          )
        ), ifNotExists = false))
    }

    it("should decompile create external table .. partitions") {
      val model = CreateExternalTable(ref = DatabaseObjectRef("revenue_per_page"),
        ExternalTable(
          columns = List(
            "rank String".c, "section String".c, "super_section String".c, "page_name String".c,
            "rev String".c, "last_processed_ts_est String".c
          ),
          options = Dictionary(
            "partitions" -> ArrayLiteral("hit_dt_est".v, "site_experience_desc".v),
            "format" -> "CSV",
            "location" -> "/web/revenue_per_page/"
          )
        ), ifNotExists = false)
      assert(model.toSQL ==
        """|create external table revenue_per_page (rank: String, section: String, super_section: String, page_name: String, rev: String, last_processed_ts_est: String)
           |containing { partitions: ["hit_dt_est", "site_experience_desc"], format: "CSV", location: "/web/revenue_per_page/" }
           |""".stripMargin.trim.replaceAll("\n", " "))
    }

  }

}
