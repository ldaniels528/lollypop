package com.qwery.runtime.instructions.jvm

import com.qwery.database.QueryResponse
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.JSONSupport.JSONStringConversion
import org.scalatest.funspec.AnyFunSpec
import spray.json.enrichAny

class InvokeSQLTest extends AnyFunSpec {

  describe(classOf[InvokeSQL].getSimpleName) {

    it("should return a result as a QueryResponse") {
      val (_, _, outcome) = QweryVM.executeSQL(Scope(),
        """|invokeSQL("select symbol: 'GMTQ', exchange: 'OTCBB', lastSale: 0.1111")
           |""".stripMargin)
      val results = outcome match {
        case q: QueryResponse => q.toRowCollection.toMapGraph
        case _ => Nil
      }
      assert(results == List(Map("symbol" -> "GMTQ", "exchange" -> "OTCBB", "lastSale" -> 0.1111)))
    }

    it("should return a result as a JSON object graph") {
      implicit val scope: Scope = Scope()
      val response = InvokeSQL("select symbol: 'GMTQ', exchange: 'OTCBB', lastSale: 0.1111".v).evaluate()
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
