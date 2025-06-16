#!/bin/bash

# Cluster Configuration
HOST_LOCAL="10.10.1.81"  # Your local node
PORT=7000
HOST_REMOTE="10.10.1.56" # Remote node with 2 instances
KEY_COUNT=20             # Number of keys to test

# Function to print with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Verify cluster status first
log "=== Verifying Cluster Status ==="
cluster_status=$(redis-cli -h $HOST_LOCAL -p $PORT cluster info | grep cluster_state)
log "Cluster state: $cluster_status"
if [[ "$cluster_status" != *"ok"* ]]; then
    log "Error: Cluster is not healthy!"
    exit 1
fi

# Insert keys and measure time
log "=== Inserting $KEY_COUNT keys ==="
start_time=$(date +%s.%N)
for i in $(seq 1 $KEY_COUNT); do
    key="key_$(printf "%02d" $i)"
    value="value_$(openssl rand -hex 8)"  # Random 16-byte value
    redis-cli -c -h $HOST_LOCAL -p $PORT SET $key $value >/dev/null
    log "Set $key → Slot: $(redis-cli -h $HOST_LOCAL -p $PORT CLUSTER KEYSLOT $key)"
done
insert_time=$(echo "$(date +%s.%N) - $start_time" | bc)
log "Insertion completed in $insert_time seconds"
# Fetch all keys and classify local/remote
log "\n=== Benchmarking Fetch Performance ==="
local_time=0
remote_time=0
local_count=0
remote_count=0

for i in $(seq 1 $KEY_COUNT); do
    key="key_$(printf "%02d" $i)"

    # Get key location
    slot=$(redis-cli -h $HOST_LOCAL -p $PORT CLUSTER KEYSLOT "$key")

    # Time the fetch operation
    start_time=$(date +%s.%N)
    redis-cli -c -h $HOST_LOCAL -p $PORT GET "$key" >/dev/null
    elapsed=$(echo "$(date +%s.%N) - $start_time" | bc)

    # Classify as local or remote
    if [ $slot -ge 5461 ] && [ $slot -le 10922 ]; then
        # Local to 10.10.1.81:7000
        local_time=$(echo "$local_time + $elapsed" | bc)
        ((local_count++))
        log "FETCH LOCAL: $key (Slot: $slot, Time: ${elapsed} sec)"
    else
        # Remote (either 10.10.1.56:7000 or 10.10.1.56:7001)
        remote_time=$(echo "$remote_time + $elapsed" | bc)
        ((remote_count++))
        log "FETCH REMOTE: $key (Slot: $slot, Time: ${elapsed} sec)"
    fi
done

# Calculate averages
avg_local=$(echo "scale=6; $local_time / $local_count" | bc)
avg_remote=$(echo "scale=6; $remote_time / $remote_count" | bc)
# Print final results
log "\n=== Benchmark Results ==="
log "Total keys inserted: $KEY_COUNT"
log "Insertion time: $insert_time seconds"
log "Local fetches: $local_count (avg: $avg_local sec)"
log "Remote fetches: $remote_count (avg: $avg_remote sec)"
log "Speed difference: $(echo "scale=2; $avg_remote/$avg_local" | bc)x slower for remote"

# Verify data distribution
log "\n=== Cluster Distribution ==="
log "Keys on $HOST_LOCAL:$PORT: $(redis-cli -h $HOST_LOCAL -p $PORT DBSIZE)"
log "Keys on $HOST_REMOTE1:$PORT: $(redis-cli -h $HOST_REMOTE1 -p $PORT DBSIZE)"
log "Keys on $HOST_REMOTE2:$PORT_REMOTE2: $(redis-cli -h $HOST_REMOTE2 -p $PORT_REMOTE2 DBSIZE)"
