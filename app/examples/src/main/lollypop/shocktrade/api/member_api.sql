node.api(port, '/api/shocktrade/members', {
    //////////////////////////////////////////////////////////////////////////////////////
    // creates a new member
    // http post 'http://{{host}}:{{port}}/api/shocktrade/members' <~ { name: "fugitive528" }
    //////////////////////////////////////////////////////////////////////////////////////
    post: (name: String) => {
        val result = insert into Members (name) values (name)
        inserted_id('Members', result)
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // retrieves a member
    // http get 'http://{{host}}:{{port}}/api/shocktrade/members?id=2187296c-7bf6-4c1d-a87d-fdc3dae39dc8'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (id: UUID) => {
        from ns('Members') where member_id is $id limit 1
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // updates a member
    // http put 'http://{{host}}:{{port}}/api/shocktrade/members' <~ { id: "0a3dd064-b3c7-4c44-aad0-c7bd94e1f929", name: "Winter is coming" }
    //////////////////////////////////////////////////////////////////////////////////////
    put: (id: UUID, newName: String) => {
        update Members set name = $newName where member_id is $id
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // deletes a member
    // http delete 'http://{{host}}:{{port}}/api/shocktrade/members' <~ { id: "0a3dd064-b3c7-4c44-aad0-c7bd94e1f929" }
    //////////////////////////////////////////////////////////////////////////////////////
    delete: (id: UUID) => {
        delete from Members where member_id is $id
    }
})

node.api(port, '/api/shocktrade/members/by/name', {
    //////////////////////////////////////////////////////////////////////////////////////
    // searches for contests by name
    // http post 'http://{{host}}:{{port}}/api/shocktrade/members/by/name' <~ { searchText: "fugitive528" }
    //////////////////////////////////////////////////////////////////////////////////////
    post: (searchText: String) => {
        from ns('Members') where name contains searchText 
    }
})