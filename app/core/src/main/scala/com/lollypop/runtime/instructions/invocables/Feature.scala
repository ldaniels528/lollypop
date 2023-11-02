package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_TESTING, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.{CodeBlock, Condition, Expression, Instruction}
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.errors.ScenarioNotFoundError
import com.lollypop.runtime.instructions.conditions.RuntimeCondition.RichConditionAtRuntime
import com.lollypop.runtime.instructions.conditions.{RuntimeCondition, Verification}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.io.IOCost
import org.slf4j.LoggerFactory

/**
 * Feature declaration
 * @param title     the feature [[Expression title]]
 * @param scenarios the feature [[Instruction scenarios]]
 */
case class Feature(title: Expression, scenarios: Seq[Instruction]) extends RuntimeInvokable {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val fs0 = FeatureState(scope)
    title.asString.foreach(title => fs0.println(s"Feature: $title"))
    val featureState = scenarios.foldLeft[FeatureState](fs0) {
      // is it a scenario?
      case (state, scenario: Scenario) =>
        val (scopeA, costA, resultA) = LollypopVM.execute(resolveScope(scenario, state), scenario)
        if (resultA == true) completedScenario(scenario, state, scopeA)
        else failedScenario(scenario, state, scopeA)
      // any other instruction
      case (state, instruction) =>
        val (scopeA, costA, _) = LollypopVM.execute(state.scope, instruction)
        state.copy(scope = scopeA)
    }
    featureState.println(s"completed: passed: ${featureState.passed}, failed: ${featureState.failed}")
    featureState.lines.foreach(logger.info)
    (featureState.scope, IOCost.empty, Map("passed" -> featureState.passed, "failed" -> featureState.failed))
  }

  private def completedScenario(scenario: Scenario, state: FeatureState, scopeA: Scope): FeatureState = {
    val title = scenario.title.asString(state.scope) || scenario.die("Scenario has no title")
    state.println(s"   Passed: $title")
    reportVerifications(scenario, state, scopeA)
    state.copy(
      scope = state.scope,
      parents = state.parents ++ Map(title -> scopeA),
      passed = state.passed + 1)
  }

  private def failedScenario(scenario: Scenario, state: FeatureState, scopeA: Scope): FeatureState = {
    val title = scenario.title.asString(state.scope) || scenario.die("Scenario has no title")
    state.println(s"   Failed: $title")
    reportVerifications(scenario, state, scopeA)
    //scopeA.toRowCollection.tabulate().foreach { line => state.println(s"     $line") }
    state.copy(scope = scopeA, failed = state.failed + 1)
  }

  private def reportVerifications(scenario: Scenario, state: FeatureState, scopeA: Scope): Unit = {
    scenario.verifications.foreach {
      case v: Verification =>
        v.title.flatMap(_.asString(scopeA)) foreach { title =>
          state.println(s"      $title")
        }
        val conditions = v.condition match {
          case c: Condition => c.collect { case c => c }
          case _ => Nil
        }
        conditions.foreach { c =>
          state.println(s"      ${if (RuntimeCondition.isTrue(c)(scopeA)) "[x]" else "[ ]"} ${c.toSQL}")
        }
      case _ =>
    }
  }

  private def resolveScope(scenario: Scenario, featureState: FeatureState)(implicit scope: Scope): Scope = {

    def getReferencedScope(title: String, titleExpr: Expression): Scope = {
      featureState.parents.getOrElse(title, throw new ScenarioNotFoundError(title, titleExpr))
    }

    scenario.inherits match {
      case Some(titleExpr) =>
        LollypopVM.execute(scope, titleExpr)._3 match {
          case title: String => getReferencedScope(title, titleExpr)
          case titles: Array[String] =>
            titles.foldLeft(scope) { case (aggScope, title) =>
              aggScope ++ getReferencedScope(title, titleExpr)
            }
          case x =>
            titleExpr.die(s"Expected either a String or String Array but got '${x.renderFriendly}' (${Option(x).map(_.getClass.getSimpleName)})")
        }
      case None => featureState.scope
    }
  }

  override def toSQL: String = {
    (s"feature ${title.toSQL} {" :: scenarios.toList.flatMap { i =>
      val sql = i.toSQL
      if (sql contains "\n") sql.split("\n").toList.map(s => s"\t$s") else List(s"\t$sql")
    } ::: "}" :: Nil).mkString("\n")
  }

  private case class FeatureState(scope: Scope,
                                  parents: Map[String, Scope] = Map.empty,
                                  passed: Int = 0,
                                  failed: Int = 0,
                                  var lines: List[String] = Nil) {
    def println(message: String): Unit = {
      scope.stdOut.println(message)
      lines = lines ::: message :: Nil
    }
  }

}

/**
 * Feature Parser
 */
object Feature extends InvokableParser {
  val template = "feature %e:title %N:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "feature",
    category = CATEGORY_TESTING,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Feature-based test declaration",
    example =
      """|namespace 'temp.examples'
         |
         |// startup a listener node
         |val port = nodeStart()
         |
         |// create a table
         |drop if exists Travelers
         |create table Travelers (id: UUID, lastName: String(32), firstName: String(32), destAirportCode: String(3))
         |insert into Travelers (id, lastName, firstName, destAirportCode)
         ||-------------------------------------------------------------------------------|
         || id                                   | lastName | firstName | destAirportCode |
         ||-------------------------------------------------------------------------------|
         || 7bd0b461-4eb9-400a-9b63-713af85a43d0 | JONES    | GARRY     | SNA             |
         || 73a3fe49-df95-4a7a-9809-0bb4009f414b | JONES    | DEBBIE    | SNA             |
         || e015fc77-45bf-4a40-9721-f8f3248497a1 | JONES    | TAMERA    | SNA             |
         || 33e31b53-b540-45e3-97d7-d2353a49f9c6 | JONES    | ERIC      | SNA             |
         || e4dcba22-56d6-4e53-adbc-23fd84aece72 | ADAMS    | KAREN     | DTW             |
         || 3879ba60-827e-4535-bf4e-246ca8807ba1 | ADAMS    | MIKE      | DTW             |
         || 3d8dc7d8-cd86-48f4-b364-d2f40f1ae05b | JONES    | SAMANTHA  | BUR             |
         || 22d10aaa-32ac-4cd0-9bed-aa8e78a36d80 | SHARMA   | PANKAJ    | LAX             |
         ||-------------------------------------------------------------------------------|
         |
         |// create the webservice that reads from the table
         |nodeAPI(port, '/api/temp/examples', {
         |  post: (id: UUID, firstName: String, lastName: String, destAirportCode: String) => {
         |     insert into Travelers (id, firstName, lastName, destAirportCode)
         |     values (@id, @firstName, @lastName, @destAirportCode)
         |  },
         |  get: (firstName: String, lastName: String) => {
         |     select * from Travelers where firstName is @firstName and lastName is @lastName
         |  },
         |  put: (id: Long, name: String) => {
         |     update subscriptions set name = @name where id is @id
         |  },
         |  delete: (id: UUID) => {
         |     delete from Travelers where id is @id
         |  }
         |})
         |
         |// test the service
         |feature "Traveler information service" {
         |    set __AUTO_EXPAND__ = true // Product classes are automatically expanded into the scope
         |    scenario "Testing that DELETE requests produce the correct result" {
         |       http delete "http://0.0.0.0:{{port}}/api/temp/examples"
         |           <~ { id: '3879ba60-827e-4535-bf4e-246ca8807ba1' }
         |       verify statusCode is 200
         |    }
         |    scenario "Testing that GET response contains specific field" {
         |       http get "http://0.0.0.0:{{port}}/api/temp/examples?firstName=GARRY&lastName=JONES"
         |       verify statusCode is 200
         |           and body.size() >= 0
         |           and body[0].id is '7bd0b461-4eb9-400a-9b63-713af85a43d0'
         |    }
         |    scenario "Testing that POST creates a new record" {
         |        http post "http://0.0.0.0:{{port}}/api/temp/examples"
         |           <~ { id: "119ff8a6-b569-4d54-80c6-03eb1c7f795d", firstName: "CHRIS", lastName: "DANIELS", destAirportCode: "DTW" }
         |        verify statusCode is 200
         |    }
         |    scenario "Testing that we GET the record we previously created" {
         |       http get "http://0.0.0.0:{{port}}/api/temp/examples?firstName=CHRIS&lastName=DANIELS"
         |       verify statusCode is 200
         |          and body matches [{
         |              id: "119ff8a6-b569-4d54-80c6-03eb1c7f795d",
         |              firstName: "CHRIS",
         |              lastName: "DANIELS",
         |              destAirportCode: "DTW"
         |          }]
         |    }
         |    scenario "Testing what happens when a response does not match the expected value" {
         |       http get "http://0.0.0.0:{{port}}/api/temp/examples?firstName=SAMANTHA&lastName=JONES"
         |       verify statusCode is 200
         |          and body.size() >= 0
         |          and body[0].id is "7bd0b461-4eb9-400a-9b63-713af85a43d1"
         |          and body[0].firstName is "SAMANTHA"
         |          and body[0].lastName is "JONES"
         |          and body[0].destAirportCode is "BUR"
         |    }
         |}
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Feature] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      val scenarios = params.instructions.get("code") match {
        case Some(CodeBlock(statements)) => statements
        case Some(statements) => List(statements)
        case None => Nil
      }
      Some(Feature(title = params.expressions("title"), scenarios))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "feature"

}