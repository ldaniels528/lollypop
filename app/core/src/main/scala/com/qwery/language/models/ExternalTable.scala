package com.qwery.language.models

/**
 * Represents a Hive-compatible external table definition.
 * <p>Table Options:</p>
 * <ul>
 * <li>delimiter       the optional field terminator/delimiter (e.g. ",")</li>
 * <li>line_terminator the optional line terminator/delimiter (e.g. ";")</li>
 * <li>headers         the optional headers indicator</li>
 * <li>null_values     the optional value to substitute for nulls (e.g. 'n/a')</li>
 * <li>format          the input file format (e.g. "csv")</li>
 * <li>location        the physical location of the data files</li>
 * </ul>
 * @param columns the table [[Column columns]]
 * @param options the table [[Expression options]]
 * @example
 * {{{
 *  create external table companyList (
 *    symbol: String(5),
 *    exchange: String(6),
 *    lastSale: Double,
 *    transactionTime: Long)
 *    containing { format: 'CSV', location: './demos/stocks.csv.gz', headers: true, delimiter: ',' }
 * }}}
 * @see [[https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_create_table.html]]
 */
case class ExternalTable(columns: List[Column], options: Expression)

