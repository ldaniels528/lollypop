package com.qwery.language.models

/**
 * Represents a table model
 * @param columns         the table [[Column columns]]
 * @param initialCapacity the optional initial capacity (in-memory tables only)
 * @param partitions      the optional [[Expression partitions]]
 * @example
 * {{{
 *     table if not exists Cars(
 *         Name: String,
 *         Miles_per_Gallon: Int,
 *         Cylinders: Int,
 *         Displacement: Int,
 *         Horsepower: Int,
 *         Weight_in_lbs: Int,
 *         Acceleration: Decimal,
 *         Year: Date,
 *         Origin: Char(1))
 * }}}
 * @example
 * {{{
 *   declare table Securities (symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)
 *   containing (
 *   |---------------------------------------------------------|
 *   | exchange | symbol | lastSale | lastSaleTime             |
 *   |---------------------------------------------------------|
 *   | AMEX     | SYTE   | 150.4262 | 2023-09-03T21:58:20.553Z |
 *   | NYSE     | UADIZ  | 111.7227 | 2023-09-03T21:57:38.086Z |
 *   | NASDAQ   | MVL    | 195.9821 | 2023-09-03T21:58:08.610Z |
 *   | NASDAQ   | SVKJ   | 103.2728 | 2023-09-03T21:57:49.566Z |
 *   | NYSE     | TG     |  97.3604 | 2023-09-03T21:57:24.790Z |
 *   | NASDAQ   | TQZQ   |  234.337 | 2023-09-03T21:57:57.617Z |
 *   | AMEX     | AHZCC  |  229.711 | 2023-09-03T21:57:39.022Z |
 *   | OTCBB    | LJJL   | 185.9484 | 2023-09-03T21:57:54.961Z |
 *   |---------------------------------------------------------|
 *   ) partition by exchange
 * }}}
 */
case class TableModel(columns: List[Column],
                      initialCapacity: Option[Int] = None,
                      partitions: Option[Expression] = None)
