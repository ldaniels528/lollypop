namespace `demo.breakout`
//////////////////////////////////////////////////////////////////////////////////////
//      BREAKOUT DEMO
// include './contrib/examples/src/main/qwery/BreakOutDemo.sql'
//////////////////////////////////////////////////////////////////////////////////////

import [
    'java.awt.Color',
    'java.awt.Dimension',
    'javax.swing.JFrame',
    'javax.swing.JPanel'
]

//////////////////////////////////////////////////////////////////////////////////////
// constants
//////////////////////////////////////////////////////////////////////////////////////

// define the block colors
val colors = [Color.GREEN, Color.MAGENTA, Color.CYAN, Color.BLUE]

// create the content pane & frame
val world_width = 800
val world_height = 600

//////////////////////////////////////////////////////////////////////////////////////
// functions
//////////////////////////////////////////////////////////////////////////////////////

def createFrame() := {
    val frame = new JFrame('BreakOut Demo')
    frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE)
    contentPane.setPreferredSize(new Dimension(world_width, world_height))
    frame.setContentPane(contentPane)
    frame.pack()
    frame.setResizable(false)
    frame.setVisible(true)
    frame
}

def generateBlocks() := {
    truncate @@blocks
    var by = 50
    while by < 250 {
        bx = 0
        while bx < world_width {
            b_color = Random.nextInt(colors.length())
            insert into @@blocks (x, y, width, height, color) values(bx, by, 40, 20, b_color)
            bx += 45
        }
        by += 25
    }
    count(@@blocks)
}

def moveBlocks() := update @@blocks y += 1

def tick() := Random.nextInt(11) % 2

//////////////////////////////////////////////////////////////////////////////////////
// initialization
//////////////////////////////////////////////////////////////////////////////////////

// define a collection for the blocks
declare table blocks(x: Int, y: Int, width: Int, height: Int, color: Int)[150]

// create the frame, content pane and image buffer
val contentPane = new JPanel(true)
val frame = createFrame()
val buffer = contentPane.createImage(world_width, world_height)
val onScreen = contentPane.getGraphics()
val offScreen = buffer.getGraphics()

// create the ball variables
let ball_x: Int = Random.nextInt(world_width)
let ball_y: Int = world_height / 2
var radius = 14
val half = radius / 4
var distance = 2.857142 // speed * ct
var direction = ['SW', 'NW'][tick()]

// create the frame rate variables
var fps = 0.0
var lastUpdate = DateTime()
var n_frames = 0

// game state variables
var level = 1
var isAlive = true
var n_bricks = 0

//////////////////////////////////////////////////////////////////////////////////////
// setup the logger
//////////////////////////////////////////////////////////////////////////////////////

import 'org.slf4j.LoggerFactory'
val logger = LoggerFactory.getLogger('BreakOutDemo')

//////////////////////////////////////////////////////////////////////////////////////
// main loop
//////////////////////////////////////////////////////////////////////////////////////

def clearScreen() := {
    offScreen.setColor(Color.BLACK)
    offScreen.fillRect(0, 0, world_width, world_height)
}

def drawBlocks() := {
    each block in @@blocks {
        offScreen.setColor(colors[color])
        offScreen.fillRect(x, y, width, height)
    }
}

def drawTheBall() := {
    offScreen.setColor(Color.YELLOW)
    offScreen.fillOval(Int(ball_x - half), Int(ball_y - half), radius, radius)
}

def drawBlockCount() := {
    offScreen.setColor(Color.WHITE)
    offScreen.drawString('level: ' + level, 20, 20)
    offScreen.drawString('blocks left: ' + n_bricks, 220, 20)
}

def drawFrameRate() := {
    offScreen.setColor(Color.GREEN)
    offScreen.drawString(String(fps.toScale(2)), 700, 20)
}

def handleCollisions() := {
    val outcome =
        delete from @@blocks
        where ball_x between (x - half) and (x + width + half)
          and ball_y between (y - half) and (y + height + half)
    val killed = outcome.deleted
    killed
}

def handledDeadBlocks() := {
    val n_bricks = count(blocks)
    if n_bricks is 0 {
        level += 1
        if level >= 3 {
            isAlive = false
            frame.dispose()
        } else {
            generateBlocks()
        }
    }
    n_bricks
}

def main(args: String[]) := {
    var n_bricks = generateBlocks()
    while isAlive is true {
        clearScreen()
        drawBlocks()
        drawTheBall()
        val killed = handleCollisions()
        if killed > 0 n_bricks = handledDeadBlocks()

        drawBlockCount()
        drawFrameRate()

        // render the scene
        onScreen.drawImage(buffer, 0, 0, frame)
        n_frames += 1

        // compute the frame rate
        val moment = DateTime()
        val diff = (moment - lastUpdate) / 1000
        if diff >= 1 {
            fps = n_frames / diff
            n_frames = 0
            lastUpdate = moment
            moveBlocks()
        }

        // move the ball
        if direction is 'NE' {
            if ball_x >= world_width direction = iff(ball_y > (world_height / 2), 'NW', ['SW', 'NW'][tick()])
            else if ball_y <= 0 direction = iff(ball_x > (world_width / 2), 'SE', ['SW', 'SE'][tick()])
            else {
               ball_x += distance
               ball_y -= distance
            }
        } else if direction is 'SE' {
            if ball_x >= world_width direction = iff(ball_y > (world_height / 2), 'SW', ['NW', 'SW'][tick()])
            else if ball_y >= world_height direction = iff(ball_x > (world_width / 2), 'NE', ['NW', 'NE'][tick()])
            else {
                ball_x += distance
                ball_y += distance
            }
        } else if direction is 'SW' {
             if ball_x <= 0 direction = iff(ball_y > (world_height / 2), 'SE', ['NE', 'SE'][tick()])
             else if ball_y >= world_height direction = iff(ball_x > (world_width / 2), 'NW', ['NE', 'NW'][tick()])
             else {
                ball_x -= distance
                ball_y += distance
             }
        } else if direction is 'NW' {
             if ball_x <= 0 direction = iff(ball_y > (world_height / 2), 'NE', ['SE', 'NE'][tick()])
             else if ball_y <= 0 direction = iff(ball_x > (world_width / 2), 'SW', ['SE', 'SW'][tick()])
             else {
                ball_x -= distance
                ball_y -= distance
             }
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////////
// startup
//////////////////////////////////////////////////////////////////////////////////////

main([])