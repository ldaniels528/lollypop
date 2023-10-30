//////////////////////////////////////////////////////////////////////////////////////
// Title: ShockTrade Demo Test
// include('./contrib/examples/src/test/lollypop/shocktrade/shocktrade_test.sql')
//
// the remote tables references:
// -----------------------------
// val Contests = ns('//{{host}}:{{port}}/{{__namespace__}}.Contests')
// val Members = ns('//{{host}}:{{port}}/{{__namespace__}}.Members')
// val Participants = ns('//{{host}}:{{port}}/{{__namespace__}}.Participants')
//////////////////////////////////////////////////////////////////////////////////////

include('./contrib/examples/src/main/lollypop/shocktrade/shocktrade.sql')

feature 'ShockTrade services' {

    // log all calls to the `http` instruction
    whenever '^http (delete|get|post|put) (.*)' {
        logger.info('{{__INSTRUCTION__}}')
        logger.info("response = {{__RETURNED__.toJsonPretty()}}")
    }

   //////////////////////////////////////////////////////////////////////////////////////
   //   Contest unit tests
   //////////////////////////////////////////////////////////////////////////////////////

    scenario 'Create a new contest' {
        val responseA = http post 'http://{{host}}:{{port}}/api/shocktrade/contests' <~ { name: "Winter is coming" }
        val contest_id = responseA.body
        verify responseA.statusCode is 200
            ^^^ "Created contest: {{contest_id}}"
    }

    scenario 'Retrieve the contest' extends 'Create a new contest' {
        val responseB = http get 'http://{{host}}:{{port}}/api/shocktrade/contests?id={{contest_id}}'
        verify responseB.statusCode is 200
            and responseB.body.size() is 1
            and responseB.body[0] matches { contest_id: @contest_id, name: "Winter is coming", funds: 2000.0, creationTime: isNotNull }
                ^^^ "Retrieved contest: {{contest_id}}"
    }
            
    scenario 'Update an existing contest' {
        // 1. create a new contest
        val responseC = http post 'http://{{host}}:{{port}}/api/shocktrade/contests' <~ { name: "Winter is here" }
        val contest_id = responseC.body
        verify responseC.statusCode is 200
            ^^^ "Created contest: {{contest_id}}"

        // 2. update the contest
        val newName = 'Winter has come!!!'
        val responseD = http put 'http://{{host}}:{{port}}/api/shocktrade/contests?newName={{url_encode(newName)}}&id={{contest_id}}'
        verify responseD.statusCode is 200 and responseD.body is 1
            ^^^ "Updated contest: {{contest_id}}"
    }

    scenario 'Search for contests' {
        val responseE = http post 'http://{{host}}:{{port}}/api/shocktrade/contests/by/name' <~ { searchText: "Winter" }
        verify responseE.statusCode is 200
            and responseE.body.size() is 2
            and responseE.body[0] matches { contest_id: isUUID, name: "Winter is coming", funds: 2000.0, creationTime: isDateTime }
            and responseE.body[1] matches { contest_id: isUUID, name: "Winter has come!!!", funds: 2000.0, creationTime: isDateTime }
                ^^^ "Retrieved contests: {{responseE.body[0].contest_id}}, {{responseE.body[1].contest_id}}"
    }

   //////////////////////////////////////////////////////////////////////////////////////
   //   Member unit tests
   //////////////////////////////////////////////////////////////////////////////////////

    scenario 'Create a new member' {
        val responseF = http post 'http://{{host}}:{{port}}/api/shocktrade/members' <~ { name: "fugitive528" }
        val memberID = responseF.body
        verify responseF.statusCode is 200
            ^^^ "Created member: {{memberID}}"
    }

    scenario 'Retrieve the member' extends 'Create a new member' {
        val responseG = http get 'http://{{host}}:{{port}}/api/shocktrade/members?id={{memberID}}'
        verify responseG.statusCode is 200
            and responseG.body.size() is 1
            and responseG.body[0] matches { member_id: memberID, name: "fugitive528", funds: 100000, creationTime: isDateTime }
                ^^^ "Retrieved member: {{memberID}}"
    }

   //////////////////////////////////////////////////////////////////////////////////////
   //   Participant unit tests
   //////////////////////////////////////////////////////////////////////////////////////

    scenario 'Join a member to a contest as a new participant' extends ['Create a new member', 'Create a new contest'] {
        val responseH = http post 'http://{{host}}:{{port}}/api/shocktrade/participants'
                            <~ { contest_id: @contest_id, member_id: @memberID }
        val participant_id = responseH.body
        verify responseH.statusCode is 200
            ^^^ "Joined contest: member {{memberID}}, participant {{participant_id}}"
    }

    scenario 'Retrieve the participant' extends 'Join a member to a contest as a new participant' {
        val responseI = http get 'http://{{host}}:{{port}}/api/shocktrade/participants?id={{participant_id}}'
        verify responseI.statusCode is 200
            and responseI.body.size() is 1
            and responseI.body[0] matches {
                contest_id: @contest_id, member_id: memberID, participant_id: @participant_id,
                funds: 2000, creationTime: isDateTime
            } ^^^ "Retrieved participant: {{participant_id}}"
    }

   //////////////////////////////////////////////////////////////////////////////////////
   //   Order unit tests
   //////////////////////////////////////////////////////////////////////////////////////

    scenario 'Create a new order' extends 'Join a member to a contest as a new participant' {
        val responseJ = http post 'http://{{host}}:{{port}}/api/shocktrade/orders'
                            <~ { contest_id: @contest_id, participant_id: @participant_id,
                                 symbol: 'AAPL', exchange: 'NYSE', order_type: 'BUY', order_terms: 'LIMIT', price: 95.67 }
        val order_id = responseJ.body
        verify responseJ.statusCode is 200
            ^^^ "Created order: {{order_id}}"
    }

   scenario 'Retrieve the order' extends 'Create a new order' {
       val responseK = http get 'http://{{host}}:{{port}}/api/shocktrade/orders?id={{order_id}}'
       verify responseK.statusCode is 200
           and responseK.body.size() is 1
           and responseK.body[0] matches {
                contest_id: @contest_id, participant_id: @participant_id, order_id: @order_id,
                symbol: 'AAPL', exchange: 'NYSE', order_type: 'BUY', order_terms: 'LIMIT', price: 95.67,
                price: 95.67, creationTime: isDateTime, expirationTime: isDateTime
           } ^^^ "Retrieved order: {{order_id}}"
   }

   //////////////////////////////////////////////////////////////////////////////////////
   //   Position unit tests
   //////////////////////////////////////////////////////////////////////////////////////

    scenario 'Create a new position' extends 'Create a new order' {
        val responseL = http post 'http://{{host}}:{{port}}/api/shocktrade/positions'
                            <~ { contest_id: @contest_id, participant_id: @participant_id, order_id: @order_id,
                                 symbol: 'AAPL', exchange: 'NYSE', pricePaid: 95.11 }
        val position_id = responseL.body
        verify responseL.statusCode is 200
            ^^^ "Created position: {{position_id}}"
    }

   scenario 'Retrieve the position' extends 'Create a new position' {
       val responseM = http get 'http://{{host}}:{{port}}/api/shocktrade/positions?id={{position_id}}'
       verify responseM.statusCode is 200
           and responseM.body.size() is 1
           and responseM.body[0] matches {
                contest_id: @contest_id, participant_id: @participant_id, order_id: @order_id, position_id: @position_id,
                symbol: 'AAPL', exchange: 'NYSE', pricePaid: 95.11, creationTime: isDateTime
           } ^^^ "Retrieved position: {{position_id}}"
   }

    scenario 'Retrieve the positions by participant' extends 'Create a new position' {
        val responseN = http get 'http://{{host}}:{{port}}/api/shocktrade/positions/by/participant?id={{participant_id}}'
        verify responseN.statusCode is 200
            and responseN.body.size() is 1
            and responseN.body[0] matches {
                contest_id: @contest_id, participant_id: @participant_id, order_id: @order_id, position_id: @position_id,
                symbol: 'AAPL', exchange: 'NYSE', pricePaid: 95.11, creationTime: isDateTime
            } ^^^ "Retrieved position: {{position_id}}"
   }
}