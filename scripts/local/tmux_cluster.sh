#!/bin/bash
# Open a tmux session for the running RainStorm Docker cluster.
#
# Layout:
#   Left  (60%): node1 interactive CLI  (docker attach node1)
#   Right top:   live cluster logs      (docker compose logs -f)
#   Right bottom: shell for host commands (make demo, docker exec, etc.)
#
# Usage:  ./scripts/local/tmux_cluster.sh
#   Requires: tmux, docker compose cluster already up (make up)

SESSION="rainstorm"

if ! command -v tmux &>/dev/null; then
    echo "tmux is not installed. Install with: brew install tmux"
    exit 1
fi

# Kill existing session if present
tmux kill-session -t "$SESSION" 2>/dev/null || true

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

tmux new-session -d -s "$SESSION" -x 220 -y 50

# Pane 0 (left, 60%): attach to node1 interactive CLI
tmux send-keys -t "$SESSION:0.0" "cd '$REPO_ROOT' && echo 'Press Enter for the hydfs> prompt, Ctrl+P Ctrl+Q to detach.' && docker attach node1" Enter

# Split right (40%): logs pane on top
tmux split-window -t "$SESSION:0.0" -h -p 40
tmux send-keys -t "$SESSION:0.1" "cd '$REPO_ROOT' && docker compose logs -f" Enter

# Split logs pane vertically: shell on bottom
tmux split-window -t "$SESSION:0.1" -v -p 35
tmux send-keys -t "$SESSION:0.2" "cd '$REPO_ROOT'" Enter

# Focus the left (node1) pane
tmux select-pane -t "$SESSION:0.0"

# Attach
tmux attach-session -t "$SESSION"
