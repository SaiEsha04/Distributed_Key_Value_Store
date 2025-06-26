#!/bin/bash

# Cluster Configuration
HOST_LOCAL="10.10.1.81"  # Your local node
PORT=7000
HOST_REMOTE="10.10.1.56" # Remote node with 2 instances
KEY_COUNT=15000          # Number of keys to test

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

# Generate random 3-letter string
generate_value() {
    cat /dev/urandom | tr -dc 'a-zA-Z' | fold -w 3 | head -n 1
}

# Function to shuffle array
shuffle_array() {
    local i tmp size max rand
    size=${#local_keys[@]}
    max=$(( 32768 / size * size ))
    for ((i=size-1; i>0; i--)); do
        while (( (rand=$RANDOM) >= max )); do :; done
        rand=$(( rand % (i+1) ))
        tmp=${local_keys[i]}
        local_keys[i]=${local_keys[rand]}
        local_keys[rand]=$tmp
    done
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
inserted_count=0
start_time=$(date +%s.%N)
while [ $inserted_count -lt $KEY_COUNT ]; do
    key=$inserted_count  # Simple integer key
    value=$(generate_value)  # Random 3-letter string
    result=$(redis-cli -c -h $HOST_LOCAL -p $PORT SET $key $value)
    if [[ "$result" == "OK" ]]; then
        ((inserted_count++))
        # Show progress every 1000 keys
        if [ $((inserted_count % 1000)) -eq 0 ]; then
            log "Inserted $inserted_count/$KEY_COUNT keys..."
        fi
    else
        log "Warning: Failed to insert key $key - $result"
    fi
done
insert_time=$(echo "$(date +%s.%N) - $start_time" | bc)
log "Insertion completed in $insert_time seconds"
log "Successfully inserted $inserted_count keys"

# Identify local keys only
log "=== Identifying Local Keys ==="
local_keys=()
remote_keys=()

for i in $(seq 0 $((KEY_COUNT-1))); do
    key=$i
    slot=$(redis-cli -h $HOST_LOCAL -p $PORT CLUSTER KEYSLOT "$key")

    if [ $slot -ge $local_slot_start ] && [ $slot -le $local_slot_end ]; then
        local_keys+=("$key")
    else
        remote_keys+=("$key")
    fi

    # Show progress every 5000 keys
    if [ $((i % 5000)) -eq 0 ]; then
        log "Processed $((i+1))/$KEY_COUNT keys..."
    fi
done

local_count=${#local_keys[@]}
remote_count=${#remote_keys[@]}
log "Found $local_count local keys and $remote_count remote keys"

# Benchmark LOCAL keys in RANDOM order
log "=== Benchmarking LOCAL Key Fetch Performance (RANDOM ORDER) ==="
local_time=0
fetch_times=()

if [ $local_count -eq 0 ]; then
    log "Error: No local keys found to benchmark!"
    exit 1
fi

# Shuffle the local keys array
log "Shuffling keys for random access..."
shuffle_array

log "Benchmarking $local_count local keys in random order..."

for key in "${local_keys[@]}"; do
    # Time the fetch operation
    start_time=$(date +%s.%N)
    result=$(redis-cli -c -h $HOST_LOCAL -p $PORT GET "$key")
    elapsed=$(echo "$(date +%s.%N) - $start_time" | bc)
    local_time=$(echo "$local_time + $elapsed" | bc)
    fetch_times+=("$elapsed")
done

# Calculate statistics
avg_local=0
min_time=${fetch_times[0]}
max_time=${fetch_times[0]}

if [ $local_count -gt 0 ]; then
    avg_local=$(echo "scale=6; $local_time / $local_count" | bc)

    for time in "${fetch_times[@]}"; do
        if (( $(echo "$time < $min_time" | bc -l) )); then
            min_time=$time
        fi
        if (( $(echo "$time > $max_time" | bc -l) )); then
            max_time=$time
        fi
    done
fi

# Print final results
log "\n=== RANDOM FETCH Benchmark Results ==="
log "Total keys inserted: $KEY_COUNT"
log "Insertion time: $insert_time seconds"
log "Local keys found: $local_count"
log "Remote keys found: $remote_count"
log "Fetched LOCAL keys randomly: $local_count"
log "Average fetch time: $avg_local seconds"
log "Minimum fetch time: $min_time seconds"
log "Maximum fetch time: $max_time seconds"

# Verify data distribution
log "\n=== Cluster Distribution ==="
log "Keys on $HOST_LOCAL:$PORT: $(redis-cli -h $HOST_LOCAL -p $PORT DBSIZE)"
log "Keys on $HOST_REMOTE:7000: $(redis-cli -h $HOST_REMOTE -p 7000 DBSIZE)"
log "Keys on $HOST_REMOTE:7001: $(redis-cli -h $HOST_REMOTE -p 7001 DBSIZE)"
