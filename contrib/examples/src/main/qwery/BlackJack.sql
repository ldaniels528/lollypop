//////////////////////////////////////////////////////////////////////////////////////
//      PLAYING CARDS - BLACKJACK DEMO
// inspired by: https://codereview.stackexchange.com/questions/82103/ascii-fication-of-playing-cards
// include('./contrib/examples/src/main/qwery/BlackJack.sql')
//////////////////////////////////////////////////////////////////////////////////////

import "java.lang.Math"

faces = explode(face: ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"])
suits = explode(suit: ["♠", "♦", "♥", "♣"])
deck = faces * suits
player = tableLike(deck)
dealer = tableLike(deck)
money = 1000.0
bet = 25.0
level = 1

//////////////////////////////////////////////////////////////////////////////////////
//      UTILITY METHODS
//////////////////////////////////////////////////////////////////////////////////////

def getCardScore(hand) := {
    def computeScore(aceScore: Int) := {
        (select score: sum(case
                           when face is "A" -> aceScore
                           when face between "2" and "9" -> Int(face)
                           else 10 end)
        from @@hand)[0][0]
    }

    val v11 = computeScore(11)
    iff(v11 <= 21, v11, computeScore(1))
}

def dealerScore() := getCardScore(dealer)

def hit(hand) := insert into @@hand from deck.pop()

def playerScore() := getCardScore(player)

def dealerIntelligence(finish: Boolean = false) := {
    var modified = false
    _playerScore = playerScore()
    if((_playerScore <= 21) and (dealerScore() < _playerScore)) {
        val cost =
            if (finish) while (_playerScore > dealerScore()) hit(dealer)
            else if(_playerScore > dealerScore()) hit(dealer)
        modified = (modified is true) or (cost.inserted > 0)
    }
    modified
}

def faceUp(face, suit) := {
    faceL = iff(face.length() < 2, face + " ", face)
    faceR = iff(face.length() < 2, " " + face, face)
"""
┌─────────┐
│ {{faceL}}    {{suit}} │
│         │
│    {{suit}}    │
│         │
│ {{suit}}    {{faceR}} │
└─────────┘
"""
}

def faceDown() := """
┌─────────┐
│░░░░░░░░░│
│░░░░░░░░░│
│░░░░░░░░░│
│░░░░░░░░░│
│░░░░░░░░░│
└─────────┘
"""

def showTitle(out) := {
  """|        _             _                                 _
     |  _ __ | | __ _ _   _(_)_ __   __ _    ___ __ _ _ __ __| |___
     | | '_ \| |/ _` | | | | | '_ \ / _` |  / __/ _` | '__/ _` / __|
     | | |_) | | (_| | |_| | | | | | (_| | | (_| (_| | | | (_| \__ \
     | | .__/|_|\__,_|\__, |_|_| |_|\__, |  \___\__,_|_|  \__,_|___/
     | |_|            |___/         |___/
     |""".stripMargin('|') ===> out
}

//////////////////////////////////////////////////////////////////////////////////////
//      MAIN PROGRAM
//////////////////////////////////////////////////////////////////////////////////////

showTitle(out)

isAlive = true
while(isAlive) {
    // reset and shuffle the deck
    deck = faces * suits
    deck.shuffle()

    // put some cards into the hands of the player and dealer
    [dealer, player].foreach(hand => { truncate @@hand; hit(hand) })

    isJousting = true
    betFactor = 1.0

    def showSeparator(out) := {
        separator = ("¤" * 120) + "\n"
        out <=== ("\n" + separator)
        out <=== " Player: {{__userName__}} \t Credit: ${{money}} \t Bet: ${{bet}} \t Round: {{level}} \n"
        out <=== (separator + "\n")
    }

    def showHand(out, cards) := {
        var lines = []
        each card in cards {
            isVisible = true
            val _card = if(isVisible) faceUp(face, suit) else faceDown()
            val lines1 = _card.split("[\n]")
            lines = iff(lines.length() == 0, lines1, (lines <|> lines1).map(a => a.join(" ")))
        }

        // add a face down card in the last position
        if (cards == dealer) {
            val lines1 = faceDown().split("[\n]")
            lines = iff(lines.length() == 0, lines1, (lines <|> lines1).map(a => a.join(" ")))
        }

        // write to STDOUT
        (lines.join("\n") + "\n") ===> out
    }

    def showGameTable(out) := {
        flag = iff(betFactor == 2.0, "2x ", "")
        "DEALER - {{dealerScore()}}/21" ===> out
        showHand(out, dealer)
        "YOU ({{__userName__}}) - {{flag}}{{playerScore()}}/21" ===> out
        showHand(out, player)
    }

    // display the hands of the player and dealer
    showSeparator(out); showGameTable(out)

    // main loop
    loop = 0
    while(isJousting) {
        showCards = false

        // check the game status
        if ((dealerScore() > 21) or (playerScore() >= 21)) isJousting = false
        else {
            out <=== """Choose {{ iff(loop == 0, "[D]ouble-down, ", "") }}[H]it, [S]tand or [Q]uit? """
            choice = stdin.readLine().trim().toUpperCase()
            if ((choice.startsWith("D") is true) and (loop == 0)) betFactor = 2.0
            else if(choice.startsWith("H")) { hit(player); showCards = true }
            else if(choice.startsWith("Q")) { isJousting = false; isAlive = false }
            else if(choice.startsWith("S")) isJousting = false
        }

        // allow the dealer to respond && compute the scores
        if (dealerIntelligence()) showCards = true
        if (showCards) showGameTable(out)
        loop += 1
    }

    def roundCompleted(out, message: String, betDelta: Double) := {
        out <=== (message + "\n")
        if (isDefined(bet)) money += betFactor * betDelta
    }

    // allow the AI one last turn
    if (dealerIntelligence(true)) showGameTable(out)

    // decide who won - https://www.officialgamerules.org/blackjack
    if (dealerScore() == playerScore()) roundCompleted(out, "Draw.", 0)
    else if (playerScore() == 21) roundCompleted(out, "You Win!! - Player BlackJack!", 1.5 * bet)
    else if (dealerScore() == 21) roundCompleted(out, "You Lose - Dealer BlackJack!", -bet)
    else if (dealerScore() > 21) roundCompleted(out, "You Win!! - Dealer Busts: {{dealerScore()}}", bet)
    else if (playerScore() > 21) roundCompleted(out, "You Lose - Player Busts: {{playerScore()}}", -bet)
    else if (playerScore() > dealerScore()) roundCompleted(out, "You Win!! - {{playerScore()}} vs. {{dealerScore()}}", -bet)
    else roundCompleted(out, "You Lose - {{dealerScore()}} vs. {{playerScore()}}", -bet)
    level += 1
}

money