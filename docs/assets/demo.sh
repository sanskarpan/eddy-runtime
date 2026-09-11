#!/usr/bin/env bash
# Scripted eddy-console demo for recording (e.g. `vhs docs/assets/demo.tape`).
#
# 1. Builds the workspace and starts a synthetic event stream.
# 2. Launches `eddy-console` against it.
# 3. Record the terminal with your tool of choice and save the GIF as
#    `docs/assets/demo.gif` (TUI overview) or `docs/assets/tui.gif`
#    (task-detail drill-down). Do not commit placeholder binaries.
set -euo pipefail
cd "$(dirname "$0")/../.."

cargo build -p eddy-console
echo "Starting synthetic event stream..."
echo "In another terminal, run: ./target/debug/eddy-console"
echo "Press Ctrl-C here to stop."
sleep infinity
