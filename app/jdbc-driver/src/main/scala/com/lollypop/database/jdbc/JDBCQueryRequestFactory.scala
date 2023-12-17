package com.lollypop.database.jdbc

import com.lollypop.database.QueryRequest
import com.lollypop.database.jdbc.types.JDBCValueConversion.JDBCStringRenderer
import com.lollypop.runtime._

import scala.collection.mutable

/**
 * JDBC Query Request Factory
 */
object JDBCQueryRequestFactory {

  def apply(sql: String, params: List[Any], limit: Option[Int]): QueryRequest = {
    val indices: List[Int] = parseSQLTemplate(sql)
    val (_sql, keyValues) = processTemplate(sql, indices, params)
    QueryRequest(_sql, keyValues, limit)
  }

  private def parseSQLTemplate(sql: String): List[Int] = {
    var params: List[Int] = Nil
    var pos = -1
    do {
      pos = sql.indexOf('?', pos + 1)
      if (pos != -1) params = pos :: params
    } while (pos != -1)
    params.reverse
  }


  private def processTemplate(sql: String, indices: List[Int], params: List[Any]): (String, Map[String, String]) = {
    assert(indices.size == params.size, die(s"Parameter count mismatch: ${indices.size} != ${params.size}"))
    case class Accum(sb: mutable.StringBuilder = new mutable.StringBuilder(sql),
                     index: Int = indices.size - 1,
                     keyValues: Map[String, String] = Map.empty)
    val acc = indices.reverse.foldLeft[Accum](Accum()) { case (acc@Accum(sb, index, keyValues), pos) =>
      params(index).renderAsStringOrBytes match {
        case Left(s) => acc.copy(sb = sb.replace(pos, pos + 1, s), index - 1)
        case Right(b) =>
          val name = s"_${keyValues.size}"
          acc.copy(
            sb = sb.replace(pos, pos + 1, name),
            index = index - 1,
            keyValues = keyValues + (name -> b.toBase64))
      }
    }
    (acc.sb.toString(), acc.keyValues)
  }

}
