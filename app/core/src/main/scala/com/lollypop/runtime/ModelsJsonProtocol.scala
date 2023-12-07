package com.lollypop
package runtime

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.lollypop.database.QueryRequest
import com.lollypop.language._
import com.lollypop.language.models._
import com.lollypop.runtime.DatabaseObjectConfig._
import com.lollypop.runtime.ModelStringRenderer.ModelStringRendering
import com.lollypop.runtime.datatypes.DataType
import com.lollypop.runtime.devices._
import com.lollypop.util.DateHelper
import lollypop.io.IOCost
import lollypop.lang.Pointer
import spray.json._

import java.util.Date

/**
 * Models JSON Protocol
 */
object ModelsJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  ////////////////////////////////////////////////////////////////////////
  //      Utility Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit object MapStringAnyJsonFormat extends JsonFormat[Map[String, Any]] {
    override def read(json: JsValue): Map[String, Any] = json.unwrapJSON match {
      case m: Map[String, Any] => m
      case o => die(s"'$o' (${Option(o).map(_.getClass.getSimpleName).orNull}) is not a Map[String, Any]")
    }

    override def write(value: Map[String, Any]): JsValue = {
      def wrap(value: Any): JsValue = value match {
        case null | None => JsNull
        case Some(v) => wrap(v)
        case v: Array[_] => JsArray(v.map(wrap): _*)
        case v: Seq[_] => JsArray(v.map(wrap): _*)
        case m: Map[String, _] => JsObject(fields = m.map { case (k, v) => k -> wrap(v) })
        case v: BigDecimal => v.toJson
        case v: BigInt => v.toJson
        case v: Boolean => v.toJson
        case v: Byte => v.toJson
        case v: Class[_] => s"""classOf("${v.getName}")""".toJson
        case v: Date => DateHelper.format(v).toJson
        case v: Double => v.toJson
        case v: Float => v.toJson
        case v: Int => v.toJson
        case v: Long => v.toJson
        case v: Short => v.toJson
        case v: String => JsString(v)
        case v: Template => v.toString.toJson
        case v => JsString(v.asModelString)
      }

      JsObject(value.map { case (name, value) => name -> wrap(value) })
    }
  }

  final implicit object ListMapStringAnyJsonFormat extends JsonFormat[List[Map[String, Any]]] {
    override def read(json: JsValue): List[Map[String, Any]] = Nil

    override def write(list: List[Map[String, Any]]): JsValue = JsArray(list.map(_.toJson): _*)
  }

  final implicit object OptionAnyJsonFormat extends JsonFormat[Option[Any]] {
    override def read(json: JsValue): Option[Any] = Option(json.unwrapJSON)

    override def write(value: Option[Any]): JsValue = value map (_.toSprayJs) getOrElse JsNull
  }

  final implicit object SeqSeqOptionAnyJsonFormat extends JsonFormat[Seq[Seq[Any]]] {
    override def read(json: JsValue): Seq[Seq[Any]] = json match {
      case JsArray(rowsJs) => rowsJs collect {
        case JsArray(colsJs) => colsJs.map(v => v.unwrapJSON)
      }
    }

    override def write(rows: Seq[Seq[Any]]): JsValue = {
      JsArray((for {row <- rows; array = JsArray(row.map(_.toSprayJs): _*)} yield array): _*)
    }
  }

  ////////////////////////////////////////////////////////////////////////
  //      DatabaseObjectRef Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit object DatabaseObjectNSJsonFormat extends JsonFormat[DatabaseObjectNS] {
    override def read(json: JsValue): DatabaseObjectNS = json match {
      case JsString(path) => DatabaseObjectRef(path) match {
        case ns: DatabaseObjectNS => ns
        case x => dieIllegalType(x)
      }
      case x => dieIllegalType(x)
    }

    override def write(ns: DatabaseObjectNS): JsValue = ns.toSQL.toJson
  }

  final implicit object DatabaseObjectRefJsonFormat extends JsonFormat[DatabaseObjectRef] {
    override def read(json: JsValue): DatabaseObjectRef = json match {
      case JsString(path) => DatabaseObjectRef(path)
      case x => dieIllegalType(x)
    }

    override def write(ref: DatabaseObjectRef): JsValue = ref.toSQL.toJson
  }

  ////////////////////////////////////////////////////////////////////////
  //      Core Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit object ColumnTypeJsonFormat extends JsonFormat[ColumnType] {
    override def read(json: JsValue): ColumnType = {
      val sql = json.convertTo[String]
      ColumnTypeParser.nextColumnType(TokenStream(sql))(LollypopCompiler())
    }

    override def write(columnType: ColumnType): JsValue = JsString(columnType.toSQL)
  }

  final implicit object expressionJsonFormat extends JsonFormat[Expression] {
    override def read(json: JsValue): Expression = {
      val sql = json.convertTo[String]
      val ts = TokenStream(sql)
      LollypopCompiler().nextExpression(ts) || die(s"Could not compile: $sql")
    }

    override def write(expression: Expression): JsValue = JsString(expression.toSQL)
  }

  final implicit val columnModelJsonFormat: RootJsonFormat[Column] = {
    jsonFormat4((name: String, `type`: ColumnType, defaultValue: Option[Expression], isRowID: Boolean) => Column.apply(name, `type`, defaultValue, isRowID))
  }

  final implicit val orderColumnJsonFormat: RootJsonFormat[OrderColumn] = jsonFormat2(OrderColumn.apply)

  ////////////////////////////////////////////////////////////////////////
  //      I/O Cost Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit object IOCostJsonFormat extends JsonFormat[IOCost] {
    override def read(json: JsValue): IOCost = json match {
      case JsObject(elements) => IOCost(elements.collect {
        case (name, n: JsNumber) => name -> n.value.toLong
      })
      case x => dieIllegalType(x)
    }

    override def write(cost: IOCost): JsValue = cost.toMap.toJson
  }

  ////////////////////////////////////////////////////////////////////////
  //      Column Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit object DataTypeJsonFormat extends JsonFormat[DataType] {
    override def read(json: JsValue): DataType = {
      implicit val scope: Scope = Scope()
      DataType.parse(json.convertTo[String])
    }

    override def write(dataType: DataType): JsValue = JsString(dataType.toSQL)
  }

  final implicit val columnJsonFormat: RootJsonFormat[TableColumn] = {
    jsonFormat3((name: String, `type`: DataType, defaultValue: Option[Expression]) => TableColumn.apply(name, `type`, defaultValue))
  }

  ////////////////////////////////////////////////////////////////////////
  //      Row Metadata Implicits
  ////////////////////////////////////////////////////////////////////////

  implicit object RowMetadataJsonFormat extends JsonFormat[RowMetadata] {
    override def read(json: JsValue): RowMetadata = json match {
      case JsNumber(value) => RowMetadata.decode(value.toByte)
    }

    override def write(rmd: RowMetadata): JsValue = JsNumber(rmd.encode.toInt)
  }

  ////////////////////////////////////////////////////////////////////////
  //      Field Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val fieldMetadataJsonFormat: RootJsonFormat[FieldMetadata] = jsonFormat2(FieldMetadata.apply)

  final implicit val fieldJsonFormat: RootJsonFormat[Field] = jsonFormat3(Field.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Config Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val externalFunctionConfigJsonFormat: RootJsonFormat[ExternalFunctionConfig] = jsonFormat2(ExternalFunctionConfig.apply)

  final implicit val externalTableConfigJsonFormat: RootJsonFormat[ExternalTableConfig] = jsonFormat6(ExternalTableConfig.apply)

  final implicit val functionConfigJsonFormat: RootJsonFormat[FunctionConfig] = jsonFormat1(FunctionConfig.apply)

  final implicit val hashIndexConfig: RootJsonFormat[HashIndexConfig] = jsonFormat2(HashIndexConfig.apply)

  final implicit val macroConfigJsonFormat: RootJsonFormat[MacroConfig] = jsonFormat2(MacroConfig.apply)

  final implicit val pointerRefJsonFormat: RootJsonFormat[Pointer] = jsonFormat3(Pointer.apply)

  final implicit val persistentVariableConfigJsonFormat: RootJsonFormat[PersistentVariableConfig] = jsonFormat3(PersistentVariableConfig.apply)

  final implicit val parameterJsonFormat: RootJsonFormat[Parameter] = jsonFormat4(Parameter.apply)

  final implicit val procedureConfigJsonFormat: RootJsonFormat[ProcedureConfig] = jsonFormat2(ProcedureConfig.apply)

  final implicit val virtualTableConfigJsonFormat: RootJsonFormat[VirtualTableConfig] = jsonFormat1(VirtualTableConfig.apply)

  final implicit val userTypeConfig: RootJsonFormat[UserDefinedTypeConfig] = jsonFormat1(UserDefinedTypeConfig.apply)

  final implicit val databaseObjectConfigJsonFormat: RootJsonFormat[DatabaseObjectConfig] = jsonFormat12(DatabaseObjectConfig.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Result Set Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val queryRequestJsonFormat: RootJsonFormat[QueryRequest] = jsonFormat3(QueryRequest.apply)

  final implicit val rowJsonFormat: RootJsonFormat[Row] = jsonFormat4(Row.apply)

  final implicit val rowStatisticsJsonFormat: RootJsonFormat[RowSummary] = jsonFormat6(RowSummary.apply)

}
