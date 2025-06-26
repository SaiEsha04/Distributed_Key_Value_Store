#!/bin/bash

# Cluster Configuration
HOST_LOCAL="10.10.1.81"  # Your local node
PORT=7000
HOST_REMOTE="10.10.1.56" # Remote node with 2 instances
KEY_COUNT=7500          # Number of keys to test (15,000 for ~5k per node)

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
    size=${#remote_keys[@]}
    max=$(( 32768 / size * size ))
    for ((i=size-1; i>0; i--)); do
        while (( (rand=$RANDOM) >= max )); do :; done
        rand=$(( rand % (i+1) ))
        tmp=${remote_keys[i]}
        remote_keys[i]=${remote_keys[rand]}
        remote_keys[rand]=$tmp
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

# Get slot ranges for all nodes
log "Getting slot ranges for all nodes..."
read local_slot_start local_slot_end <<< $(get_node_slots $HOST_LOCAL $PORT)
read remote1_slot_start remote1_slot_end <<< $(get_node_slots $HOST_REMOTE 7000)
read remote2_slot_start remote2_slot_end <<< $(get_node_slots $HOST_REMOTE 7001)

log "Slot ranges:"
log "Local node ($HOST_LOCAL:$PORT): $local_slot_start - $local_slot_end"
log "Remote node1 ($HOST_REMOTE:7000): $remote1_slot_start - $remote1_slot_end"
log "Remote node2 ($HOST_REMOTE:7001): $remote2_slot_start - $remote2_slot_end"

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

# Identify remote keys only
log "=== Identifying Remote Keys ==="
remote_keys=()
remote1_keys=()
remote2_keys=()

for i in $(seq 0 $((KEY_COUNT-1))); do
    key=$i
    slot=$(redis-cli -h $HOST_LOCAL -p $PORT CLUSTER KEYSLOT "$key")

    if [ $slot -ge $remote1_slot_start ] && [ $slot -le $remote1_slot_end ]; then
        remote_keys+=("$key")
        remote1_keys+=("$key")
    elif [ $slot -ge $remote2_slot_start ] && [ $slot -le $remote2_slot_end ]; then
        remote_keys+=("$key")
        remote2_keys+=("$key")
    fi

    # Show progress every 5000 keys
    if [ $((i % 5000)) -eq 0 ]; then
        log "Processed $((i+1))/$KEY_COUNT keys..."
    fi
done

remote_count=${#remote_keys[@]}
remote1_count=${#remote1_keys[@]}
remote2_count=${#remote2_keys[@]}
log "Found $remote_count remote keys total:"
log " - $remote1_count keys on $HOST_REMOTE:7000"
log " - $remote2_count keys on $HOST_REMOTE:7001"

# Benchmark REMOTE keys in RANDOM order
log "=== Benchmarking REMOTE Key Fetch Performance (RANDOM ORDER) ==="
remote_time=0
remote_fetch_times=()

if [ $remote_count -eq 0 ]; then
    log "Error: No remote keys found to benchmark!"
    exit 1
fi

# Shuffle the remote keys array
log "Shuffling keys for random access..."
shuffle_array

log "Benchmarking $remote_count remote keys in random order..."

for key in "${remote_keys[@]}"; do
    # Time the fetch operation
    start_time=$(date +%s.%N)
    result=$(redis-cli -c -h $HOST_LOCAL -p $PORT GET "$key")
    elapsed=$(echo "$(date +%s.%N) - $start_time" | bc)
    remote_time=$(echo "$remote_time + $elapsed" | bc)
    remote_fetch_times+=("$elapsed")
done

# Calculate statistics
avg_remote=0
min_time=${remote_fetch_times[0]}
max_time=${remote_fetch_times[0]}

if [ $remote_count -gt 0 ]; then
    avg_remote=$(echo "scale=6; $remote_time / $remote_count" | bc)

    for time in "${remote_fetch_times[@]}"; do
        if (( $(echo "$time < $min_time" | bc -l) )); then
            min_time=$time
        fi
        if (( $(echo "$time > $max_time" | bc -l) )); then
            max_time=$time
        fi
    done
fi

# Print final results
log "\n=== RANDOM REMOTE FETCH Benchmark Results ==="
log "Total keys inserted: $KEY_COUNT"
log "Insertion time: $insert_time seconds"
log "Remote keys found: $remote_count"
log " - $HOST_REMOTE:7000 keys: $remote1_count"
log " - $HOST_REMOTE:7001 keys: $remote2_count"
log "Fetched REMOTE keys randomly: $remote_count"
log "Average fetch time: $avg_remote seconds"
log "Minimum fetch time: $min_time seconds"
log "Maximum fetch time: $max_time seconds"

# Verify data distribution
log "\n=== Cluster Distribution ==="
log "Keys on $HOST_LOCAL:$PORT: $(redis-cli -h $HOST_LOCAL -p $PORT DBSIZE)"
log "Keys on $HOST_REMOTE:7000: $(redis-cli -h $HOST_REMOTE -p 7000 DBSIZE)"
log "Keys on $HOST_REMOTE:7001: $(redis-cli -h $HOST_REMOTE -p 7001 DBSIZE)"
