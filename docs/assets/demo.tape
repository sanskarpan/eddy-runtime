Output docs/assets/demo.gif
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
# Workers + steal heatmap
Type "2"
Sleep 3s
# Resources (mutex holder + waiters)
Type "3"
Sleep 3s
# Async ops
Type "4"
Sleep 3s
# Back to tasks, sort by busy, pause the feed
Type "1"
Sleep 1s
Type "b"
Sleep 3s
Type " "
Sleep 2s
Type "?"
Sleep 3s
Type "?"
Sleep 1s
Type "q"
Sleep 1s
