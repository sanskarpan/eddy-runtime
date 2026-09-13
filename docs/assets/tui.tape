Output docs/assets/tui.gif
Set Shell bash
Set FontSize 16
Set Width 1280
Set Height 800
Set Framerate 30
Set Padding 16
Set Theme "Catppuccin Mocha"

Hide
Type "./target/debug/eddy-console --replay docs/assets/demo.bin"
Enter
Sleep 3s
# Drill into a task, then filter the list
Down
Down
Enter
Sleep 4s
Escape
Sleep 1s
Type "/echo"
Sleep 3s
Escape
Sleep 1s
Type "q"
Sleep 1s
