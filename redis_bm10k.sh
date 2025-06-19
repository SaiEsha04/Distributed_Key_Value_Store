#!/bin/bash

# Cluster Configuration
HOST_LOCAL="10.10.1.81"  # Your local node
PORT=7000
HOST_REMOTE="10.10.1.56" # Remote node with 2 instances
KEY_COUNT=10000           # Number of keys to test

# Function to print with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to get node slots
get_node_slots() {
    local host=$1
    local port=$2
    redis-cli -h $host -p $port cluster nodes | grep myself | awk '{print $9}' | tr '-' ' ' | awk '{print $1, $2}'
}

# Verify cluster status first
log "=== Verifying Cluster Status ==="
cluster_status=$(redis-cli -h $HOST_LOCAL -p $PORT cluster info | grep cluster_state)
log "Cluster state: $cluster_status"
if [[ "$cluster_status" != *"ok"* ]]; then
    log "Error: Cluster is not healthy!"
    exit 1
fi

# Get slot ranges for local node
read local_slot_start local_slot_end <<< $(get_node_slots $HOST_LOCAL $PORT)
log "Local node slots: $local_slot_start - $local_slot_end"

# Insert keys and measure time
log "=== Inserting $KEY_COUNT keys ==="
start_time=$(date +%s.%N)
for i in $(seq 1 $KEY_COUNT); do
    key=$i  # Simple integer key
    value="val"  # Simple 3-letter value
    redis-cli -c -h $HOST_LOCAL -p $PORT SET $key $value >/dev/null
done
insert_time=$(echo "$(date +%s.%N) - $start_time" | bc)
log "Insertion completed in $insert_time seconds"

# Fetch all keys and classify local/remote
log "=== Benchmarking Fetch Performance ==="
local_time=0
remote_time=0
local_count=0
remote_count=0

for i in $(seq 1 $KEY_COUNT); do
    key=$i  # Simple integer key
    slot=$(redis-cli -h $HOST_LOCAL -p $PORT CLUSTER KEYSLOT "$key")

    # Time the fetch operation
    start_time=$(date +%s.%N)
    redis-cli -c -h $HOST_LOCAL -p $PORT GET "$key" >/dev/null
    elapsed=$(echo "$(date +%s.%N) - $start_time" | bc)

    # Classify as local or remote
    if [ $slot -ge $local_slot_start ] && [ $slot -le $local_slot_end ]; then
        local_time=$(echo "$local_time + $elapsed" | bc)
        ((local_count++))
    else
        remote_time=$(echo "$remote_time + $elapsed" | bc)
        ((remote_count++))
    fi
done

# Calculate averages
avg_local=0
avg_remote=0
if [ $local_count -gt 0 ]; then
    avg_local=$(echo "scale=6; $local_time / $local_count" | bc)
fi
if [ $remote_count -gt 0 ]; then
    avg_remote=$(echo "scale=6; $remote_time / $remote_count" | bc)
fi

# Print final results
log "\n=== Benchmark Results ==="
log "Total keys inserted: $KEY_COUNT"
log "Insertion time: $insert_time seconds"
log "Local fetches: $local_count (avg: $avg_local sec)"
log "Remote fetches: $remote_count (avg: $avg_remote sec)"
if (( $(echo "$avg_local > 0" | bc -l) )); then
    speed_diff=$(echo "scale=2; $avg_remote/$avg_local" | bc)
    log "Speed difference: ${speed_diff}x slower for remote"
fi

# Verify data distribution
log "\n=== Cluster Distribution ==="
log "Keys on $HOST_LOCAL:$PORT: $(redis-cli -h $HOST_LOCAL -p $PORT DBSIZE)"
log "Keys on $HOST_REMOTE:7000: $(redis-cli -h $HOST_REMOTE -p 7000 DBSIZE)"
log "Keys on $HOST_REMOTE:7001: $(redis-cli -h $HOST_REMOTE -p 7001 DBSIZE)"
