nodeAPI(port, '/api/shocktrade/positions', {
    //////////////////////////////////////////////////////////////////////////////////////
    // creates a new position
    // http post 'http://{{host}}:{{port}}/api/shocktrade/positions'
    //      <~ { symbol: @symbol, exchange: @exchange, pricePaid: @pricePaid,
    //           contest_id: @contest_id, participant_id: @participant_id, order_id: @order_id }
    //////////////////////////////////////////////////////////////////////////////////////
    post: (symbol: String, exchange: String, pricePaid: Double, contest_id: UUID, participant_id: UUID, order_id: UUID) => {
        val result = insert into Positions (symbol, exchange, pricePaid, contest_id, participant_id, order_id)
                     values (symbol, exchange, pricePaid, contest_id, participant_id, order_id)
        inserted_id('Positions', result)
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // retrieves a position
    // http get 'http://{{host}}:{{port}}/api/shocktrade/positions?id=2187296c-7bf6-4c1d-a87d-fdc3dae39dc8'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (id: UUID) => {
        from ns('Positions') where position_id is @id limit 1
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // updates a position
    // http put 'http://{{host}}:{{port}}/api/shocktrade/positions' <~ { id: "0a3dd064-b3c7-4c44-aad0-c7bd94e1f929", name: "Winter is coming" }
    //////////////////////////////////////////////////////////////////////////////////////
    put: (id: UUID, newName: String) => {
        update Positions set name = @newName where position_id is @id
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // deletes a position
    // http delete 'http://{{host}}:{{port}}/api/shocktrade/positions' <~ { id: "0a3dd064-b3c7-4c44-aad0-c7bd94e1f929" }
    //////////////////////////////////////////////////////////////////////////////////////
    delete: (id: UUID) => {
        delete from Positions where position_id is @id
    }
})

nodeAPI(port, '/api/shocktrade/positions/by/contest', {
    //////////////////////////////////////////////////////////////////////////////////////
    // searches for positions by contest ID
    // http get 'http://{{host}}:{{port}}/api/shocktrade/positions/by/contest?id=aa440939-89cb-4ba1-80b6-20100ba6a286'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (id: UUID) => {
        from ns('Positions') where contest_id is @id
    }
})

nodeAPI(port, '/api/shocktrade/positions/by/order', {
    //////////////////////////////////////////////////////////////////////////////////////
    // searches for positions by order ID
    // http get 'http://{{host}}:{{port}}/api/shocktrade/positions/by/order?id=fad8f33b-18c1-416d-ae8f-3b309d3f9589'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (id: UUID) => {
        from ns('Positions') where order_id is @id
    }
})

nodeAPI(port, '/api/shocktrade/positions/by/participant', {
    //////////////////////////////////////////////////////////////////////////////////////
    // searches for positions by participant ID
    // http get 'http://{{host}}:{{port}}/api/shocktrade/positions/by/participant?id=fad8f33b-18c1-416d-ae8f-3b309d3f9589'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (id: UUID) => {
        from ns('Positions') where participant_id is @id
    }
})