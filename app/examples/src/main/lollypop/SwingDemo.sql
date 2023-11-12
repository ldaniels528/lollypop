namespace `demo.swing`
//////////////////////////////////////////////////////////////////////////////////////
//      SWING DEMO
// include('./app/examples/src/main/lollypop/SwingDemo.sql')
//////////////////////////////////////////////////////////////////////////////////////

import ['java.awt.Color', 'java.awt.Dimension', 'javax.swing.JFrame', 'javax.swing.JPanel']

// setup the dimensions for the content pane
world_width = 800
world_height = 600
dimensions = new Dimension(world_width, world_height)

// create the content pane
contentPane = new JPanel(true)
contentPane.setPreferredSize(dimensions)

// create the frame and put the content pane into it
frame = new JFrame('Swing Demo')
frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
frame.setContentPane(contentPane)
frame.pack()
frame.setResizable(false)
frame.setVisible(true)

// create the offscreen image buffer
buffer = contentPane.createImage(world_width, world_height)
offScreen = buffer.getGraphics()
onScreen = contentPane.getGraphics()

def changeScreenColor(color) := {
    offScreen.setColor(color)
    offScreen.fillRect(0, 0, world_width, world_height)
    onScreen.drawImage(buffer, 0, 0, frame)
}

// every 3 seconds, change the screen color to RED
val timerA =
    every Duration('3 seconds')
        changeScreenColor(Color.RED)

// after an initial delay of 1 second,
// every 3 seconds change the screen color to GREEN
val timerB =
    after Duration('1 seconds')
        every Duration('3 seconds')
            changeScreenColor(Color.GREEN)

// after an initial delay of 2 seconds,
// every 3 seconds change the screen color to BLUE
val timerC =
    after Duration('2 seconds')
        every Duration('3 seconds')
            changeScreenColor(Color.BLUE)

after Duration('5 seconds') {
    timerA.cancel()
    timerB.cancel()
    timerC.cancel()
    frame.dispose()
}
