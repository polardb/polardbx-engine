#!/bin/bash

# Default values
DEFAULT_INTERVAL=60
DEFAULT_USER="root"
DEFAULT_HOST="127.0.0.1"
DEFAULT_PORT="3306"
DEFAULT_DB="transfer_test"

# --- Function Definitions ---

# Prints a log message with a timestamp.
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Displays usage information and exits.
usage() {
    echo "Usage: $0 --config <path> --pid-file <path> [options]"
    echo ""
    echo "Mandatory Arguments:"
    echo "  --config      Path to the transfer program's configuration file."
    echo "  --pid-file    Path to the PID file of the running mysqld process."
    echo ""
    echo "Database Connection Options:"
    echo "  --user        Database user (default: $DEFAULT_USER)."
    echo "  --host        Database host (default: $DEFAULT_HOST)."
    echo "  --port        Database port (default: $DEFAULT_PORT)."
    echo "  --db          Database name (default: $DEFAULT_DB)."
    echo ""
    echo "Test Control Options:"
    echo "  --interval    Average interval in seconds between kills (default: $DEFAULT_INTERVAL)."
    exit 1
}

# --- Main Logic ---

# 1. Parse command-line arguments
CONFIG_FILE=""
PID_FILE=""
INTERVAL=$DEFAULT_INTERVAL
DB_USER=$DEFAULT_USER
DB_HOST=$DEFAULT_HOST
DB_PORT=$DEFAULT_PORT
DB_NAME=$DEFAULT_DB
TRANSFER_PID=""

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --config)   CONFIG_FILE="$2"; shift ;;
        --pid-file) PID_FILE="$2"; shift ;;
        --user)     DB_USER="$2"; shift ;;
        --host)     DB_HOST="$2"; shift ;;
        --port)     DB_PORT="$2"; shift ;;
        --db)       DB_NAME="$2"; shift ;;
        --interval) INTERVAL="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

DSN="${DB_USER}@tcp(${DB_HOST}:${DB_PORT})/${DB_NAME}"

if [ -z "$CONFIG_FILE" ] || [ -z "$PID_FILE" ]; then
    log "ERROR: --config and --pid-file are mandatory."
    usage
fi

if [ ! -f "$PID_FILE" ];
then
    log "ERROR: PID file not found at '$PID_FILE'. Ensure mysqld is running and the path is correct."
    exit 1
fi

cleanup() {
    log "Script interrupted. Cleaning up..."
    if [ -n "$TRANSFER_PID" ] && ps -p $TRANSFER_PID > /dev/null; then
        log "Killing transfer process (PID: $TRANSFER_PID)..."
        kill $TRANSFER_PID
    fi
    log "Exiting."
    exit 0
}
trap cleanup SIGINT SIGTERM

LOG_FILE="transfer.log"
> "$LOG_FILE"

log "====== Chaos Test Started ======"
log "Config: $CONFIG_FILE | PID File: $PID_FILE | Avg Interval: $INTERVAL s"
log "Constructed DSN for transfer tool: $DSN"

log "Running initial data preparation..."
./transfer prepare -config="$CONFIG_FILE" -dsn="$DSN" >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    log "ERROR: 'transfer prepare' failed. Check '$LOG_FILE' for details. Aborting."
    exit 1
fi
log "Data preparation complete."
echo "------------------------------------------------------------" >> "$LOG_FILE"


# 3. Start the main chaos loop
while true; do
    RANDOM_OFFSET=$(($RANDOM % 21 - 10))
    CRASH_DELAY=$(($INTERVAL + $RANDOM_OFFSET))
    if [ "$CRASH_DELAY" -lt 1 ]; then
        CRASH_DELAY=1
    fi
    ./transfer run -config="$CONFIG_FILE" -dsn="$DSN" >> "$LOG_FILE" 2>&1 &
    TRANSFER_PID=$!
    log "Transfer process started with PID: $TRANSFER_PID, crash after $CRASH_DELAY seconds..."
    sleep $CRASH_DELAY

    # --- MODIFIED: The order and logic of killing processes is changed ---

    # 1. First, gracefully stop the transfer process and wait for it to exit.
    kill -SIGTERM $TRANSFER_PID
    wait $TRANSFER_PID
    log "Transfer process has exited."
    TRANSFER_PID="" # Clear the PID variable

    # 2. Now, kill the mysqld process.
    MYSQLD_PID=$(cat "$PID_FILE")
    if ! [[ "$MYSQLD_PID" =~ ^[0-9]+$ ]]; then
        log "ERROR: Invalid content in PID file '$PID_FILE'. Got '$MYSQLD_PID'. Skipping kill."
    else
        log "Killing -9 mysqld $MYSQLD_PID"
        kill -9 "$MYSQLD_PID"
        if [ $? -ne 0 ]; then
            log "WARN: 'kill -9' command failed. Process might have already died."
        fi
    fi
    # --- END OF MODIFICATION ---

    RECOVERY_CHECK_INTERVAL=1 # seconds
    while true; do
        mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" "$DB_NAME" -e "CREATE TABLE IF NOT EXISTS _chaos_test_recovery (id INT); DROP TABLE _chaos_test_recovery;" 2>/dev/null
        
        if [ $? -eq 0 ]; then
            break
        else
            echo -n "."
            sleep $RECOVERY_CHECK_INTERVAL
        fi
    done
    echo ""

    log "Resuming chaos loop..."
    echo "----------------- CYCLE RESTARTED -----------------" >> "$LOG_FILE"
done

