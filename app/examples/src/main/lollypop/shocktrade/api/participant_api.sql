node.api(port, '/api/shocktrade/participants', {
    //////////////////////////////////////////////////////////////////////////////////////
    // member joins a contest as a participant
    // http post 'http://{{host}}:{{port}}/api/shocktrade/participants' <~ { contest_id: $contest_id, member_id: $member_id }
    //////////////////////////////////////////////////////////////////////////////////////
    post: (contest_id: UUID, member_id: UUID) => {
        // 1. retrieve the contest entry fee
        val resultC = select funds from Contests where contest_id is $contest_id
        val contestAmt = resultC[0].funds

        // 2. deduct the entry fee from the member
        val resultM = update Members set funds = funds - contestAmt where member_id is $member_id and funds >= contestAmt

        // 3. create the participant
        if (resultM.updated() == 1) {
            val resultP = insert into Participants (contest_id, member_id, funds) values (contest_id, member_id, contestAmt)
            inserted_id('Participants', resultP)
        } else null
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // retrieves a participant
    // http get 'http://{{host}}:{{port}}/api/shocktrade/participants?id=2187296c-7bf6-4c1d-a87d-fdc3dae39dc8'
    //////////////////////////////////////////////////////////////////////////////////////
    get: (id: UUID) => {
        from ns('Participants') where participant_id is $id limit 1
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // updates a participant
    // http put 'http://{{host}}:{{port}}/api/shocktrade/participants' <~ { id: "0a3dd064-b3c7-4c44-aad0-c7bd94e1f929", name: "Winter is coming" }
    //////////////////////////////////////////////////////////////////////////////////////
    put: (id: UUID, name: String) => {
        update Participants set name = $name where participant_id is $id
    },

    //////////////////////////////////////////////////////////////////////////////////////
    // participant quits a contest
    // http delete 'http://{{host}}:{{port}}/api/shocktrade/participants' <~ { id: "0a3dd064-b3c7-4c44-aad0-c7bd94e1f929" }
    //////////////////////////////////////////////////////////////////////////////////////
    delete: (id: UUID) => {
        delete from Participants where member_id is $id
    }
})