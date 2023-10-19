package com.qwery.database

/**
 * Represents a Query Request
 * @param sql       the parameterized SQL query (e.g. "select * from stocks where symbol is _0")
 * @param keyValues the parameter key-values pairs
 * @param limit     the optional results limit
 */
case class QueryRequest(sql: String, keyValues: Map[String, String], limit: Option[Int])
