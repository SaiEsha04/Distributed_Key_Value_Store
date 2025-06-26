#!/bin/bash

# Cluster Configuration
HOST_LOCAL="10.10.1.81"   # Your local node
PORT_LOCAL=7000
HOST_REMOTE="10.10.1.56"  # Remote node with 2 instances
PORT_REMOTE1=7000
PORT_REMOTE2=7001
TOTAL_KEYS=15000          # Total keys to insert (5k expected per node)

# Function to print with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Generate random 3-letter string
generate_value() {
    cat /dev/urandom | tr -dc 'a-zA-Z' | fold -w 3 | head -n 1
}

# Verify cluster status
log "=== Verifying Cluster Status ==="
cluster_status=$(redis-cli -h $HOST_LOCAL -p $PORT_LOCAL cluster info | grep cluster_state)
log "Cluster state: $cluster_status"
if [[ "$cluster_status" != *"ok"* ]]; then
    log "Error: Cluster is not healthy!"
    exit 1
fi

# Get slot ranges for all nodes
log "=== Getting Slot Ranges ==="
read local_slot_start local_slot_end <<< $(redis-cli -h $HOST_LOCAL -p $PORT_LOCAL cluster nodes | grep myself | awk '{print $9}' | tr '-' ' ' | awk '{print $1, $2}')
read remote1_slot_start remote1_slot_end <<< $(redis-cli -h $HOST_REMOTE -p $PORT_REMOTE1 cluster nodes | grep myself | awk '{print $9}' | tr '-' ' ' | awk '{print $1, $2}')
read remote2_slot_start remote2_slot_end <<< $(redis-cli -h $HOST_REMOTE -p $PORT_REMOTE2 cluster nodes | grep myself | awk '{print $9}' | tr '-' ' ' | awk '{print $1, $2}')

log "Local node slots: $local_slot_start - $local_slot_end"
log "Remote node 1 slots: $remote1_slot_start - $remote1_slot_end"
log "Remote node 2 slots: $remote2_slot_start - $remote2_slot_end"

# Initialize counters and timers
declare -a local_insert_times
local_keys_inserted=0
remote_keys_inserted=0

log "=== Starting Key Insertion ==="
total_start_time=$(date +%s.%N)

for ((i=1; i<=$TOTAL_KEYS; i++)); do
    key="$i"
    value=$(generate_value)

    # Determine target node
    slot=$(redis-cli -h $HOST_LOCAL -p $PORT_LOCAL CLUSTER KEYSLOT "$key")

    if (( slot >= local_slot_start && slot <= local_slot_end )); then
        # Time local insertion
        start_time=$(date +%s.%N)
        result=$(redis-cli -c -h $HOST_LOCAL -p $PORT_LOCAL SET "$key" "$value")
        elapsed=$(echo "$(date +%s.%N) - $start_time" | bc)
        local_insert_times+=("$elapsed")
        ((local_keys_inserted++))
        node="local"
    elif (( slot >= remote1_slot_start && slot <= remote1_slot_end )); then
        # Remote node 1
        start_time=$(date +%s.%N)
        result=$(redis-cli -c -h $HOST_REMOTE -p $PORT_REMOTE1 SET "$key" "$value")
        elapsed=$(echo "$(date +%s.%N) - $start_time" | bc)
        ((remote_keys_inserted++))
        node="remote1"
    else
        # Remote node 2
        start_time=$(date +%s.%N)
        result=$(redis-cli -c -h $HOST_REMOTE -p $PORT_REMOTE2 SET "$key" "$value")
        elapsed=$(echo "$(date +%s.%N) - $start_time" | bc)
        ((remote_keys_inserted++))
        node="remote2"
    fi

    # Progress tracking
    if (( i % 1000 == 0 )); then
        log "Inserted $i/$TOTAL_KEYS keys... (Local: $local_keys_inserted, Remote: $remote_keys_inserted)"
    fi
done

total_time=$(echo "$(date +%s.%N) - $total_start_time" | bc)

# Calculate local insertion statistics
local_total_time=0
local_min_time=0
local_max_time=0
local_avg_time=0

if (( ${#local_insert_times[@]} > 0 )); then
    local_min_time=${local_insert_times[0]}
    local_max_time=${local_insert_times[0]}

    for time in "${local_insert_times[@]}"; do
        local_total_time=$(echo "$local_total_time + $time" | bc)

        if (( $(echo "$time < $local_min_time" | bc -l) )); then
            local_min_time=$time
        fi

        if (( $(echo "$time > $local_max_time" | bc -l) )); then
            local_max_time=$time
        fi
    done

    local_avg_time=$(echo "scale=6; $local_total_time / ${#local_insert_times[@]}" | bc)
fi

log "=== Insertion Results ==="
log "Total keys inserted: $TOTAL_KEYS"
log "Total insertion time: $total_time seconds"
log "Local keys inserted: $local_keys_inserted"
log "Remote keys inserted: $remote_keys_inserted"
log "--- Local Insertion Metrics ---"
log "Total local insertion time: $local_total_time seconds"
log "Average local insertion time: $local_avg_time seconds"
log "Minimum local insertion time: $local_min_time seconds"
log "Maximum local insertion time: $local_max_time seconds"

# Verify distribution
log "=== Cluster Distribution Verification ==="
log "Keys on local node ($HOST_LOCAL:$PORT_LOCAL): $(redis-cli -h $HOST_LOCAL -p $PORT_LOCAL DBSIZE)"
log "Keys on remote node 1 ($HOST_REMOTE:$PORT_REMOTE1): $(redis-cli -h $HOST_REMOTE -p $PORT_REMOTE1 DBSIZE)"
log "Keys on remote node 2 ($HOST_REMOTE:$PORT_REMOTE2): $(redis-cli -h $HOST_REMOTE -p $PORT_REMOTE2 DBSIZE)"
