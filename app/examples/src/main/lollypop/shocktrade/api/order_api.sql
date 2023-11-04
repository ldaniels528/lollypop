nodeAPI(port, '/api/shocktrade/orders', {
    //////////////////////////////////////////////////////////////////////////////////////
    // creates a new order
    // http post 'http://{{host}}:{{port}}/api/shocktrade/orders'
    //      <~ { symbol: $symbol, exchange: $exchange, contest_id: $contest_id, participant_id: $participant_id,
    //           order_type: $order_type, order_terms: $order_terms, price: $price }
    //////////////////////////////////////////////////////////////////////////////////////
    post: (symbol: String, exchange: String, contest_id: UUID, participant_id: UUID,
                    order_type: String, order_terms: String, price: Double) => {
        val result = insert into Orders (symbol, exchange, contest_id, participant_id, order_type, order_terms, price)
                     values (symbol, exchange, contest_id, participant_id, order_type, order_terms, price)
        inserted_id('Orders', result)
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // retrieves an order
    // http get 'http://{{host}}:{{port}}/api/shocktrade/orders?id=2187296c-7bf6-4c1d-a87d-fdc3dae39dc8'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (id: UUID) => {
        from ns('Orders') where order_id is $id limit 1
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // updates an order
    // http put 'http://{{host}}:{{port}}/api/shocktrade/orders' <~ { id: "0a3dd064-b3c7-4c44-aad0-c7bd94e1f929", name: "Winter is coming" }
    //////////////////////////////////////////////////////////////////////////////////////
    put: (id: UUID, newName: String) => {
        update Orders set name = $newName where order_id is $id
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // deletes an order
    // http delete 'http://{{host}}:{{port}}/api/shocktrade/orders' <~ { id: "0a3dd064-b3c7-4c44-aad0-c7bd94e1f929" }
    //////////////////////////////////////////////////////////////////////////////////////
    delete: (id: UUID) => {
        delete from Orders where order_id is $id
    }
})
    
nodeAPI(port, '/api/shocktrade/orders/by/contest', {
    //////////////////////////////////////////////////////////////////////////////////////
    // searches for orders by contest ID
    // http get 'http://{{host}}:{{port}}/api/shocktrade/orders/by/contest?id=aa440939-89cb-4ba1-80b6-20100ba6a286'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (id: UUID) => {
        from ns('Orders') where contest_id is $id
    }
})

nodeAPI(port, '/api/shocktrade/orders/by/participant', {
    //////////////////////////////////////////////////////////////////////////////////////
    // searches for orders by participant ID
    // http get 'http://{{host}}:{{port}}/api/shocktrade/orders/by/participant?id=fad8f33b-18c1-416d-ae8f-3b309d3f9589'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (id: UUID) => {
        from ns('Orders') where participant_id is $id
    }
})