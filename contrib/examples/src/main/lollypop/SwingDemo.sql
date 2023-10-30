namespace `demo.swing`
//////////////////////////////////////////////////////////////////////////////////////
//      SWING DEMO
// include './contrib/examples/src/main/lollypop/SwingDemo.sql'
//////////////////////////////////////////////////////////////////////////////////////

import ['java.awt.Color',
        'java.awt.Dimension',
        'javax.swing.JFrame',
        'javax.swing.JPanel']

// create the content pane & frame
world_width = 800
world_height = 600
contentPane = new JPanel(true)
dimensions = new Dimension(world_width, world_height)
frame = new JFrame('Swing Demo')

// initialize the content pane & frame
contentPane.setPreferredSize(new Dimension(world_width, world_height))
frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
frame.setContentPane(contentPane)
frame.pack()
frame.setResizable(false)
frame.setVisible(true)

// create the image buffer
buffer = contentPane.createImage(world_width, world_height)
offScreen = buffer.getGraphics()
onScreen = contentPane.getGraphics()

every '3 seconds' {
    offScreen.setColor(Color.RED)
    offScreen.fillRect(0, 0, world_width, world_height)
    onScreen.drawImage(buffer, 0, 0, frame)
}

after '1 seconds' every '3 seconds' {
    offScreen.setColor(Color.GREEN)
    offScreen.fillRect(0, 0, world_width, world_height)
    onScreen.drawImage(buffer, 0, 0, frame)
}

after '2 seconds' every '3 seconds' {
    offScreen.setColor(Color.BLUE)
    offScreen.fillRect(0, 0, world_width, world_height)
    onScreen.drawImage(buffer, 0, 0, frame)
}
