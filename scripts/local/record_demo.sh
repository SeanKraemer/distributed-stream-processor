#!/bin/bash
# Scripted walkthrough used to record the README demo GIF.
#
# Tours the whole stack in one take: cluster boot + SWIM membership (MP2),
# distributed log query (MP1), HyDFS replication (MP3), and a RainStorm
# streaming job that survives a mid-run task kill with exactly-once results
# (MP4). Waits are silent so the GIF encoder can collapse idle time.
#
# Record with:
#   docker compose down -v   # clean slate first
#   asciinema rec --cols 110 --rows 30 -c ./scripts/local/record_demo.sh demo.cast
#   agg --idle-time-limit 2 --speed 1.4 --font-size 16 demo.cast docs/demo.gif

set -uo pipefail
export TERM="${TERM:-xterm-256color}"

C_HEAD='\033[1;36m'   # cyan bold — section captions
C_PROMPT='\033[1;32m' # green bold — fake prompt
C_OK='\033[1;32m'
C_WARN='\033[1;33m'
C_OFF='\033[0m'

say()  { printf "\n${C_HEAD}# %s${C_OFF}\n" "$*"; sleep 1.2; }
run()  { printf "${C_PROMPT}\$${C_OFF} %s\n" "$1"; sleep 0.6; eval "$1"; sleep 1.2; }

wait_quiet() { # poll a condition silently so idle-collapse can eat the wait
    local timeout=$1; shift
    local elapsed=0
    until eval "$@" >/dev/null 2>&1; do
        sleep 2; elapsed=$((elapsed + 2))
        [ "$elapsed" -ge "$timeout" ] && return 1
    done
    return 0
}

clear
printf "\n${C_HEAD}  Distributed Stream Processor — full-stack demo${C_OFF}\n"
printf "  SWIM failure detection · HyDFS replicated storage · exactly-once streaming\n"
printf "  10-node cluster, one machine, one command\n"
sleep 3

say "Boot the 10-node cluster"
run "make up"
wait_quiet 60 'docker logs node1 2>&1 | grep -q "Membership size: 10\|(10 members)"'
sleep 2

say "SWIM membership: every node holds the full list, no leader needed (detection <6s)"
run "docker exec node1 sh -c 'echo list_mem | nc -w 3 localhost 8003'"

say "Distributed log query: grep any node's logs remotely"
run "docker exec node5 sh -c 'echo \"dgrep_query -c joinGroup\" | nc -w 3 localhost 8003'"

say "HyDFS: upload a dataset — replicated to 3 nodes on the consistent-hash ring"
run "docker exec node1 sh -c 'echo \"create /app/data/dataset1.csv dataset1.csv\" | nc -w 5 localhost 8003'"
run "docker exec node1 sh -c 'echo \"ls dataset1.csv\" | nc -w 3 localhost 8003' | head -8"

say "RainStorm: submit a streaming job — filter 'STOP' signs, count by message (exactly-once)"
run "docker exec node1 ./rainstorm-cli 2 3 grep --pattern=STOP --column=4 count dataset1.csv out.txt true false 100 10 50 2>&1 | tail -2"
SUBMIT_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

say "While it runs... kill a live worker task. Mid-stream."
sleep 6
KILL_TARGET=$(docker exec node1 sh -c 'echo "{\"type\":\"list_tasks\",\"sender\":\"client\",\"payload\":{}}" | nc -w 5 localhost 8002' 2>/dev/null \
    | jq -r '.tasks[]? | select((.op_exe|contains("grep")) and .state=="running" and .pid>0) | "\(.vm)|\(.pid)"' | head -1)
VM="${KILL_TARGET%%|*}"; PID="${KILL_TARGET##*|}"
if [ -n "$KILL_TARGET" ]; then
    run "docker exec $VM kill -9 $PID   # kill grep task (pid $PID) on $VM"
    sleep 5
    say "The leader detects the failure and restarts the task on another node:"
    docker logs node1 --since "$SUBMIT_TS" 2>&1 | grep -E "TASK FAILED|TASK RESTART" | sed 's/.*\[LEADER\]/  [LEADER]/' | head -2
    sleep 2
else
    printf "${C_WARN}(no running grep task found — job may have finished early)${C_OFF}\n"
fi

say "Wait for the job to finish..."
wait_quiet 120 'docker logs node1 --since "$SUBMIT_TS" 2>&1 | grep -q "RUN END"'
sleep 1

say "Results — despite the kill: exact counts, zero duplicates (exactly-once)"
RESULTS=""
for n in $(seq 1 10); do
    OUT=$(docker exec "node$n" sh -c 'for f in /app/rainstorm_outputs/*/output_*.txt; do [ -s "$f" ] && cat "$f"; done' 2>/dev/null || true)
    [ -n "$OUT" ] && RESULTS="${RESULTS}${OUT}"$'\n'
done
RESULTS=$(printf '%s' "$RESULTS" | sed '/^$/d')
printf '%s\n' "$RESULTS" | awk -F',' '{count=$NF; NF--; printf "  %5d  %s\n", count, $0}' OFS=',' | sort -rn | head -6
printf "   ...\n"
TOTAL=$(printf '%s\n' "$RESULTS" | awk -F',' '{s+=$NF} END {print s}')
UNIQUE=$(printf '%s\n' "$RESULTS" | wc -l | tr -d ' ')
DUPES=$(printf '%s\n' "$RESULTS" | sort | uniq -d | wc -l | tr -d ' ')
sleep 1
printf "\n  Total: ${C_OK}%s${C_OFF} (expected 34)   Unique keys: ${C_OK}%s${C_OFF} (expected 14)   Duplicates: ${C_OK}%s${C_OFF}\n" "$TOTAL" "$UNIQUE" "$DUPES"
if [ "$TOTAL" = "34" ] && [ "$UNIQUE" = "14" ] && [ "$DUPES" = "0" ]; then
    printf "  ${C_OK}PASS — exactly-once semantics held through a task failure${C_OFF}\n"
fi
sleep 3

printf "\n${C_HEAD}  github.com/SeanKraemer/distributed-stream-processor${C_OFF}\n"
printf "  make up && make demo\n"
sleep 3
