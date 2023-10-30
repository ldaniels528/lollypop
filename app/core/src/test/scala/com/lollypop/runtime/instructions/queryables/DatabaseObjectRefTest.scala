package com.lollypop.runtime.instructions.queryables

import com.lollypop.AppConstants._
import com.lollypop.language.models.@@@
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.{DatabaseObjectNS, DatabaseObjectRef, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

/**
 * Object reference Test Suite
 */
class DatabaseObjectRefTest extends AnyFunSpec {

  describe(classOf[DatabaseObjectRef].getSimpleName) {

    it("should parse: 'securities'") {
      implicit val scope: Scope = Scope()
      assert(DatabaseObjectRef("securities").realize ==
        DatabaseObjectRef(databaseName = DEFAULT_DATABASE, schemaName = DEFAULT_SCHEMA, name = "securities"))
    }

    it("should parse: 'securities' (unapply)") {
      assert(DatabaseObjectRef.unapply(DatabaseObjectRef("securities")) contains(None, None, "securities", None))
    }

    it("should parse: 'securities#transactions' (unrealized)") {
      assert(DatabaseObjectRef("securities#transactions") ==
        DatabaseObjectRef.InnerTable(DatabaseObjectRef.Unrealized(name = "securities"), name = "transactions"))
    }

    it("should parse: 'securities#transactions' (realized)") {
      implicit val scope: Scope = Scope()
      assert(DatabaseObjectRef("securities#transactions").realize ==
        DatabaseObjectNS(databaseName = DEFAULT_DATABASE, schemaName = DEFAULT_SCHEMA, name = "securities", columnName = Some("transactions")))
    }

    it("should parse: 'securities#transactions' (unapply)") {
      assert(DatabaseObjectRef.unapply(DatabaseObjectRef("securities#transactions")) contains(None, None, "securities", Some("transactions")))
    }

    it("should parse: 'securities#transactions' (SubTable.unapply/realized)") {
      implicit val scope: Scope = Scope()
      assert(DatabaseObjectRef.SubTable.unapply(DatabaseObjectRef("securities#transactions").realize) contains
        (DatabaseObjectNS(databaseName = DEFAULT_DATABASE, schemaName = DEFAULT_SCHEMA, name = "securities"), "transactions"))
    }

    it("should parse: 'securities#transactions' (SubTable.unapply/unrealized)") {
      assert(DatabaseObjectRef.SubTable.unapply(DatabaseObjectRef("securities#transactions")) contains
        (DatabaseObjectRef.Unrealized(name = "securities"), "transactions"))
    }

    it("should parse: 'mortgage.securities'") {
      implicit val scope: Scope = Scope()
      assert(DatabaseObjectRef("mortgage.securities").realize ==
        DatabaseObjectRef(databaseName = DEFAULT_DATABASE, schemaName = "mortgage", name = "securities"))
    }

    it("should parse: 'mortgage.securities' (unapply)") {
      assert(DatabaseObjectRef.unapply(DatabaseObjectRef("mortgage.securities")) contains
        (None, Some("mortgage"), "securities", None))
    }

    it("should parse: 'finance.mortgage.securities'") {
      implicit val scope: Scope = Scope()
      assert(DatabaseObjectRef("finance.mortgage.securities").realize ==
        DatabaseObjectRef(databaseName = "finance", schemaName = "mortgage", name = "securities"))
    }

    it("should parse: 'finance.mortgage.securities' (unapply)") {
      assert(DatabaseObjectRef.unapply(DatabaseObjectRef("finance.mortgage.securities")) contains
        (Some("finance"), Some("mortgage"), "securities", None))
    }

    it("should parse: 'finance.mortgage.securities#transactions'") {
      assert(DatabaseObjectRef("finance.mortgage.securities#transactions") ==
        DatabaseObjectRef(databaseName = "finance", schemaName = "mortgage", name = "securities", columnName = "transactions"))
    }

    it("should parse: 'finance.mortgage.securities#transactions' (unapply)") {
      assert(DatabaseObjectRef.unapply(DatabaseObjectRef("finance.mortgage.securities#transactions")) contains
        (Some("finance"), Some("mortgage"), "securities", Some("transactions")))
    }

    it("should parse: '@@securities#transactions'") {
      assert(DatabaseObjectRef("@@securities#transactions") ==
        DatabaseObjectRef(@@@("securities"), name = "transactions"))
    }

    it("should parse: 'stocks' (with 'USE samples')") {
      implicit val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        """|namespace 'samples'
           |""".stripMargin)
      assert(DatabaseObjectRef("stocks").realize ==
        DatabaseObjectRef(databaseName = "samples", schemaName = DEFAULT_SCHEMA, name = "stocks"))
    }

    it("should parse: 'securities.stocks' (with 'USE samples')") {
      implicit val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        """|namespace 'samples'
           |""".stripMargin)
      assert(DatabaseObjectRef("securities.stocks").realize ==
        DatabaseObjectRef(databaseName = "samples", schemaName = "securities", name = "stocks"))
    }

    it("should parse: 'finance.mortgage.securities' (with 'USE sample.stocks')") {
      implicit val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        """|namespace 'sample.stocks'
           |""".stripMargin
      )
      assert(DatabaseObjectRef("finance.mortgage.securities").realize ==
        DatabaseObjectRef(databaseName = "finance", schemaName = "mortgage", name = "securities"))
    }

    it("should parse: 'finance.mortgage.securities#transcations' (with 'USE sample.stocks')") {
      implicit val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        """|namespace 'sample.stocks'
           |""".stripMargin
      )
      assert(DatabaseObjectRef("finance.mortgage.securities#transcations").realize ==
        DatabaseObjectRef(databaseName = "finance", schemaName = "mortgage", name = "securities", columnName = "transcations"))
    }

    it("should render 'securities' as SQL") {
      assert(DatabaseObjectRef("securities").toSQL == "securities")
    }

    it("should render 'mortgage.securities' as SQL") {
      assert(DatabaseObjectRef(schemaName = "mortgage", name = "securities").toSQL == "mortgage.securities")
    }

    it("should render 'finance.mortgage.securities' as SQL") {
      assert(DatabaseObjectRef(databaseName = "finance", schemaName = "mortgage", name = "securities").toSQL == "finance.mortgage.securities")
    }

    it("should render 'securities#transactions' as SQL") {
      assert(DatabaseObjectRef.InnerTable(DatabaseObjectRef("securities"), name = "transactions").toSQL ==
        "securities#transactions")
    }

    it("should render 'mortgage.securities#transactions' as SQL") {
      assert(DatabaseObjectRef.InnerTable(DatabaseObjectRef(schemaName = "mortgage", name = "securities"), name = "transactions").toSQL ==
        "mortgage.securities#transactions")
    }

    it("should render 'finance.mortgage.securities#transactions' as SQL") {
      assert(DatabaseObjectRef(databaseName = "finance", schemaName = "mortgage", name = "securities", columnName = "transactions").toSQL ==
        "finance.mortgage.securities#transactions")
    }

  }

}
