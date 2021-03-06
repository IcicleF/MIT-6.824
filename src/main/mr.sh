#!/bin/bash
app=wc

SESSION="mr"
COORDINATION="go run -race mrcoordinator.go pg-*.txt"
WORKER="sleep 1; go run -race mrworker.go ${app}.so"

# rebuild if necessary
go build -race -buildmode=plugin ../mrapps/wc.go

tmux new -s ${SESSION} -d &&\
# for coordinator
tmux split-window -h -p 80 -t ${SESSION}:0.0 &&\
# for workers
tmux split-window -v -p 66 -t ${SESSION}:0.1 &&\
tmux split-window -v -p 33 -t ${SESSION}:0.2 &&\

tmux send -t ${SESSION}:0.0 "${COORDINATION}" Enter &&\
tmux send -t ${SESSION}:0.1 "${WORKER}" Enter &&\
tmux send -t ${SESSION}:0.2 "${WORKER}" Enter &&\
tmux send -t ${SESSION}:0.3 "${WORKER}" Enter &&\
tmux a -t ${SESSION}

tmux kill-session -t ${SESSION}
