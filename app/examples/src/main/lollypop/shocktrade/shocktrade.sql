//////////////////////////////////////////////////////////////////////////////////////
// Title: ShockTrade Demo
// include('./app/examples/src/main/lollypop/shocktrade/shocktrade.sql')
//////////////////////////////////////////////////////////////////////////////////////
namespace 'examples.shocktrade'

require ['org.slf4j:slf4j-api:2.0.5']

import [
    'java.net.URLEncoder',
    'org.slf4j.LoggerFactory'
]

// get the logger
val logger = LoggerFactory.getLogger('ShockTradeDemo')
logger.info('Starting ShockTrade Demo Service...')

//////////////////////////////////////////////////////////////////////////////////////
// create the tables
//////////////////////////////////////////////////////////////////////////////////////

// StockQuotes table
include('./app/examples/src/main/lollypop/shocktrade/tables/StockQuotes.sql')

// Members table
include('./app/examples/src/main/lollypop/shocktrade/tables/Members.sql')

// Contests table
include('./app/examples/src/main/lollypop/shocktrade/tables/Contests.sql')

// Participants table
include('./app/examples/src/main/lollypop/shocktrade/tables/Participants.sql')

// Orders table
include('./app/examples/src/main/lollypop/shocktrade/tables/Orders.sql')

// Positions table
include('./app/examples/src/main/lollypop/shocktrade/tables/Positions.sql')

//////////////////////////////////////////////////////////////////////////////////////
// startup a server
//////////////////////////////////////////////////////////////////////////////////////

val host = '0.0.0.0'
val port = nodeStart(8080)
logger.info('Started ShockTrade Demo Service on {{host}}:{{port}}...')

// setup the web socket handler
nodeAPI(port, '/ws/shocktrade', {
  ws: (message: String) => {
    val js = message.fromJson()
    return upsert into StockQuotes (symbol, exchange, lastSale, lastSaleTime)
           values (js.symbol, js.exchange, js.lastSale, js.lastSaleTime)
           where symbol is js.symbol
  }
})

// Common Utilities
include('./app/examples/src/main/lollypop/shocktrade/utils/commons.sql')

// Stock APIs
include('./app/examples/src/main/lollypop/shocktrade/api/stock_api.sql')

// Member APIs
include('./app/examples/src/main/lollypop/shocktrade/api/member_api.sql')

// Contest APIs
include('./app/examples/src/main/lollypop/shocktrade/api/contest_api.sql')

// Contest Participants APIs
include('./app/examples/src/main/lollypop/shocktrade/api/participant_api.sql')

// Contest Orders APIs
include('./app/examples/src/main/lollypop/shocktrade/api/order_api.sql')

// Contest Positions APIs
include('./app/examples/src/main/lollypop/shocktrade/api/position_api.sql')

