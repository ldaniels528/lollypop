package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.conditions.{AND, Is, Verify}
import com.lollypop.runtime.instructions.expressions._
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import lollypop.io.Nodes
import org.scalatest.funspec.AnyFunSpec

/**
 * Feature Test Suite
 */
class FeatureTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()
  private val (databaseName, schemaName) = ("test", "TravelerService") // show `test.TravelerService.Travelers`

  // start the server
  private val node = Nodes().start()
  private val port = node.port

  describe(classOf[Feature].getSimpleName) {

    it("should compile feature statements") {
      val model = compiler.compile(
        s"""|feature "Traveler information service" {
            |    scenario "Testing that GET response contains specific field" {
            |       http get "http://0.0.0.0:$port/api/$databaseName/$schemaName?firstName=GARRY&lastName=JONES" ~> {
            |          `Content-Type`: "application/json; charset=UTF-8"
            |       }
            |       verify status is 200
            |       and response_string is '[{"id":"7bd0b461-4eb9-400a-9b63-713af85a43d0","lastName":"JONES","firstName":"GARRY","destAirportCode":"SNA"}]'
            |       and body[0].id is "7bd0b461-4eb9-400a-9b63-713af85a43d0"
            |    }
            |}
            |""".stripMargin)
      assert(model ==
        Feature(title = "Traveler information service".v, scenarios = List(
          Scenario(title = "Testing that GET response contains specific field".v, verifications = List(
            Http(method = "get", url = s"http://0.0.0.0:$port/api/$databaseName/$schemaName?firstName=GARRY&lastName=JONES".v,
              headers = Some(Dictionary("Content-Type" -> "application/json; charset=UTF-8".v))
            ),
            Verify(condition = AND(Is("status".f, 200.v),
              AND(Is("response_string".f, """[{"id":"7bd0b461-4eb9-400a-9b63-713af85a43d0","lastName":"JONES","firstName":"GARRY","destAirportCode":"SNA"}]""".v),
                Is(Infix(ElementAt("body".f, 0.v), "id".f), "7bd0b461-4eb9-400a-9b63-713af85a43d0".v)
              ))
            ))
          ))
        ))
    }

    it("should decompile feature statements") {
      val model = Feature(title = "Traveler information service".v, scenarios = List(
        Scenario(title = "Testing that GET response contains specific field".v, verifications = List(
          Http(method = "get", url = s"http://0.0.0.0:$port/api/$databaseName/$schemaName?firstName=GARRY&lastName=JONES".v,
            headers = Some(Dictionary("Content-Type" -> "application/json; charset=UTF-8".v)
            )),
          Verify(condition = AND(Is("status".f, 200.v),
            AND(Is("response_string".f, """[{ "id": "7bd0b461-4eb9-400a-9b63-713af85a43d0", "lastName": "JONES", "firstName": "GARRY","destAirportCode": "SNA" }]""".v),
              Is(Infix(ApplyTo("body".f, 0.v), "id".f), "7bd0b461-4eb9-400a-9b63-713af85a43d0".v)
            )))
        ))
      ))
      info(model.toSQL)
      assert(model.toSQL ==
        s"""|feature "Traveler information service" {
            |	scenario "Testing that GET response contains specific field" {
            |		http get "http://0.0.0.0:$port/api/$databaseName/$schemaName?firstName=GARRY&lastName=JONES" ~> { "Content-Type": "application/json; charset=UTF-8" }
            |		verify (status is 200) and ((response_string is '[{ "id": "7bd0b461-4eb9-400a-9b63-713af85a43d0", "lastName": "JONES", "firstName": "GARRY","destAirportCode": "SNA" }]') and (body(0).id is "7bd0b461-4eb9-400a-9b63-713af85a43d0"))
            |	}
            |}""".stripMargin)
    }

    it("should compile feature statements with a body") {
      val model = compiler.compile(
        s"""|feature "Traveler information service" {
            |    scenario "Testing that POST response contains specific field" {
            |       http post "http://0.0.0.0:$port/api/$databaseName/$schemaName"
            |          <~ { firstName: "GARRY", lastName: "JONES" }
            |       verify status is 200
            |       and body[0].id is "7bd0b461-4eb9-400a-9b63-713af85a43d0"
            |    }
            |}
            |""".stripMargin)
      assert(model ==
        Feature(title = "Traveler information service".v, scenarios = List(
          Scenario(title = "Testing that POST response contains specific field".v, verifications = List(
            Http(method = "post", url = s"http://0.0.0.0:$port/api/$databaseName/$schemaName".v,
              body = Some(Dictionary("firstName" -> "GARRY".v, "lastName" -> "JONES".v))
            ),
            Verify(condition = AND(Is("status".f, 200.v),
              Is(Infix(ElementAt("body".f, 0.v), "id".f), "7bd0b461-4eb9-400a-9b63-713af85a43d0".v)
            )))
          ))
        ))
    }

    it("should decompile feature statements with a body") {
      val feature = Feature(title = "Traveler information service".v, scenarios = List(
        Scenario(title = "Testing that POST response contains specific field".v, verifications = List(
          Http(method = "post", url = s"http://0.0.0.0:$port/api/$databaseName/$schemaName".v,
            body = Some(Dictionary("firstName" -> "GARRY".v, "lastName" -> "JONES".v))
          ),
          Verify(condition = AND(Is("status".f, 200.v),
            Is(Infix(ApplyTo("body".f, 0.v), "id".f), "7bd0b461-4eb9-400a-9b63-713af85a43d0".v)
          )))
        )))
      assert(feature.toSQL ===
        s"""|feature "Traveler information service" {
            |	scenario "Testing that POST response contains specific field" {
            |		http post "http://0.0.0.0:$port/api/$databaseName/$schemaName" <~ { firstName: "GARRY", lastName: "JONES" }
            |		verify (status is 200) and (body(0).id is "7bd0b461-4eb9-400a-9b63-713af85a43d0")
            |	}
            |}""".stripMargin.trim)
    }

    it("should support multiple request methods") {
      val (_, _, result) = LollypopVM.executeSQL(Scope().withVariable("node", node),
        s"""|namespace '$databaseName.$schemaName'
            |
            |// create a table
            |drop if exists Travelers
            |create table Travelers (id UUID, lastName String(32), firstName String(32), destAirportCode String(3))
            |insert into Travelers (id, lastName, firstName, destAirportCode)
            |values ('7bd0b461-4eb9-400a-9b63-713af85a43d0', 'JONES', 'GARRY', 'SNA'),
            |       ('73a3fe49-df95-4a7a-9809-0bb4009f414b', 'JONES', 'DEBBIE', 'SNA'),
            |       ('e015fc77-45bf-4a40-9721-f8f3248497a1', 'JONES', 'TAMERA', 'SNA'),
            |       ('33e31b53-b540-45e3-97d7-d2353a49f9c6', 'JONES', 'ERIC', 'SNA'),
            |       ('e4dcba22-56d6-4e53-adbc-23fd84aece72', 'ADAMS', 'KAREN', 'DTW'),
            |       ('3879ba60-827e-4535-bf4e-246ca8807ba1', 'ADAMS', 'MIKE', 'DTW'),
            |       ('3d8dc7d8-cd86-48f4-b364-d2f40f1ae05b', 'JONES', 'SAMANTHA', 'BUR'),
            |       ('22d10aaa-32ac-4cd0-9bed-aa8e78a36d80', 'SHARMA', 'PANKAJ', 'LAX')
            |
            |// create the webservice that reads from the table
            |node.api('/api/$databaseName/$schemaName', {
            |  post: (id: UUID, firstName: String, lastName: String, destAirportCode: String) => {
            |     insert into Travelers (id, firstName, lastName, destAirportCode)
            |     values ($$id, $$firstName, $$lastName, $$destAirportCode)
            |  },
            |  get: (firstName: String, lastName: String) => {
            |     select * from Travelers where firstName is $$firstName and lastName is $$lastName
            |  },
            |  put: (id: Long, name: String) => {
            |     update subscriptions set name = $$name where id is $$id
            |  },
            |  delete: (id: UUID) => {
            |     delete from Travelers where id is $$id
            |  }
            |})
            |
            |// test the service
            |feature "Traveler information service" {
            |    set __AUTO_EXPAND__ = true // Product classes are automatically expanded into the scope
            |    scenario "Testing that DELETE requests produce the correct result" {
            |       http delete "http://0.0.0.0:$port/api/$databaseName/$schemaName"
            |           <~ { id: '3879ba60-827e-4535-bf4e-246ca8807ba1' }
            |       verify statusCode is 200
            |    }
            |    scenario "Testing that GET response contains specific field" {
            |       http get "http://0.0.0.0:$port/api/$databaseName/$schemaName?firstName=GARRY&lastName=JONES"
            |       verify statusCode is 200
            |           and body.size() >= 0
            |           and body[0].id is '7bd0b461-4eb9-400a-9b63-713af85a43d0'
            |    }
            |    scenario "Testing that POST creates a new record" {
            |        http post "http://0.0.0.0:$port/api/$databaseName/$schemaName"
            |           <~ { id: "119ff8a6-b569-4d54-80c6-03eb1c7f795d", firstName: "CHRIS", lastName: "DANIELS", destAirportCode: "DTW" }
            |        verify statusCode is 200
            |    }
            |    scenario "Testing that we GET the record we previously created" {
            |       http get "http://0.0.0.0:$port/api/$databaseName/$schemaName?firstName=CHRIS&lastName=DANIELS"
            |       verify statusCode is 200
            |          and body matches [{
            |             id: "119ff8a6-b569-4d54-80c6-03eb1c7f795d",
            |             firstName: "CHRIS",
            |             lastName: "DANIELS",
            |             destAirportCode: "DTW"
            |          }]
            |    }
            |    scenario "Testing what happens when a response does not match the expected value" {
            |       http get "http://0.0.0.0:$port/api/$databaseName/$schemaName?firstName=SAMANTHA&lastName=JONES"
            |       verify statusCode is 200
            |          and body.size() >= 0
            |          and body[0].id is "7bd0b461-4eb9-400a-9b63-713af85a43d1"
            |          and body[0].firstName is "SAMANTHA"
            |          and body[0].lastName is "JONES"
            |          and body[0].destAirportCode is "BUR"
            |    }
            |}
            |""".stripMargin)
      assert(result == Map("passed" -> 4, "failed" -> 1))
    }

  }

}
