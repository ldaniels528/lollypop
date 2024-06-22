package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.Instruction

/**
 * Instruction Chain
 */
object InstructionChain extends InstructionPostfixParser[Instruction] {
  private val _symbol = "&&"

  /**
   * Parses the next instruction modifier (e.g. "where lastSale <= 1")
   * @param stream      the given [[TokenStream token stream]]
   * @param instruction the source [[Instruction instruction]]
   * @return a new instruction having the specified instruction
   * @example @stocks where lastSale <= 1 order by symbol limit 5
   */
  def apply(stream: TokenStream, instruction: Instruction)(implicit compiler: SQLCompiler): Instruction = {
    val t0 = stream.peek
    compiler.ctx.getInstructionChain(stream, instruction).map(_.tag(t0)) || instruction
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = _symbol,
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = _symbol,
    description = "Binds multiple statements together",
    example =
      """|declare table if not exists TradingSystem (
         |  stock_id: RowNumber,
         |  symbol: String(5),
         |  exchange: Enum ('AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHEROTC'),
         |  lastSale: Double,
         |  lastSaleTime: DateTime = DateTime())
         |&& insert into TradingSystem (symbol, exchange, lastSale, lastSaleTime)
         |   values ("MSFT", "NYSE", 56.55, DateTime()),
         |          ("AAPL", "NASDAQ", 98.55, DateTime()),
         |          ("AMZN", "NYSE", 56.55, DateTime()),
         |          ("GOOG", "NASDAQ", 98.55, DateTime())
         |&& from TradingSystem
         |""".stripMargin
  ))

  override def parseInstructionChain(stream: TokenStream, host: Instruction)(implicit compiler: SQLCompiler): Option[Instruction] = {
    import com.lollypop.runtime.implicits.risky._
    stream match {
      case ts if ts nextIf _symbol =>
        val chainedOp = compiler.nextOpCodeOrDie(ts)
        host match {
          case ChainedCodeBlock(ops) => ChainedCodeBlock(ops ::: List(chainedOp))
          //case InlineCodeBlock(ops) => ChainedCodeBlock(ops ::: List(opB))
          case it => ChainedCodeBlock(it, chainedOp)
        }
      case _ => host
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is _symbol

}
