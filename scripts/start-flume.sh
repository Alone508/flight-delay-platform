#!/usr/bin/env bash
# 启动 flightAgent 与 weatherAgent（后台），PID 写入 data/logs/*.pid
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"
LOGDIR=$PROJECT_DIR/data/logs
mkdir -p "$LOGDIR"
touch "$LOGDIR/flight_events.log" "$LOGDIR/weather_events.log"

start_agent() {
  local name=$1 conf=$2 pidfile="$LOGDIR/$1.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    echo "$name already running (pid $(cat "$pidfile"))"
    return 0
  fi
  nohup "$FLUME_HOME/bin/flume-ng" agent \
    --conf "$FLUME_HOME/conf" \
    --conf-file "$conf" \
    --name "$name" \
    -Dflume.root.logger=INFO,console \
    > "$LOGDIR/$name.out" 2>&1 &
  echo $! > "$pidfile"
  echo "$name started (pid $!)"
}

start_agent flightAgent "$PROJECT_DIR/flume/flight-agent.conf"
start_agent weatherAgent "$PROJECT_DIR/flume/weather-agent.conf"
sleep 3
jps -l | grep -E 'flume.node.Application' || { echo "Flume 未启动，请查看 $LOGDIR/*.out"; exit 1; }
echo "OK"
