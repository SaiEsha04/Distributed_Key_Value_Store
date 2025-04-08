#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <filesystem>
#include <thread>
#include <chrono>
#include <boost/functional/hash.hpp>
#include <hiredis/hiredis.h>

namespace fs = std::filesystem;

const std::vector<std::string> REDIS_NODES = {"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"};
const std::string WATCH_DIR = "."; 

std::map<std::string, std::string> fileToNode;  
std::map<std::string, size_t> nodeFileSizes;    

// Connect to a Redis node
redisContext* connectToRedis(const std::string& redisAddress) {
    size_t colonPos = redisAddress.find(":");
    std::string host = redisAddress.substr(0, colonPos);
    int port = std::stoi(redisAddress.substr(colonPos + 1));

    redisContext* context = redisConnect(host.c_str(), port);
    if (context == nullptr || context->err) {
        std::cerr << " Error: Unable to connect to Redis at " << redisAddress << std::endl;
        return nullptr;
    }
    return context;
}

// Find the Redis node with the least total file size load
std::string getLeastLoadedNode() {
    std::string leastLoadedNode = REDIS_NODES[0];
    size_t minLoad = nodeFileSizes[leastLoadedNode];

    for (const auto& node : REDIS_NODES) {
        if (nodeFileSizes[node] < minLoad) {
            minLoad = nodeFileSizes[node];
            leastLoadedNode = node;
        }
    }
    return leastLoadedNode;
}

// Store file content in Redis
void storeInRedis(const std::string& filePath, const std::string& node) {
    redisContext* context = connectToRedis(node);
    if (!context) return;

    
    std::string fileName = fs::path(filePath).filename().string();

    
    std::ifstream file(filePath);
    if (!file) {
        std::cerr << " Error: Unable to open file " << filePath << std::endl;
        return;
    }
    
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    
    // Store in Redis (filename as key, file content as value)
    redisReply* reply = (redisReply*)redisCommand(context, "SET %s %b", fileName.c_str(), content.c_str(), content.size());

    if (reply) {
        std::cout << "Stored File: " << fileName << " -> Redis Node: " << node << std::endl;
        freeReplyObject(reply);
    } else {
        std::cerr << " Error storing file in Redis: " << fileName << std::endl;
    }

    redisFree(context);
}

// Print current node load information
void printNodeLoad() {
    std::cout << "\n Current Redis Node Load:\n";
    for (const auto& node : REDIS_NODES) {
        std::cout << "  " << node << ": " << nodeFileSizes[node] / 1024.0 << " KB" << std::endl;
    }
    std::cout << "-----------------------------------\n";
}

// Process files and distribute them to Redis nodes
void processFiles() {
    while (true) {
        for (const auto& entry : fs::directory_iterator(WATCH_DIR)) {
            std::string filePath = entry.path().string();

           
            if (!fs::is_regular_file(entry) || fileToNode.find(filePath) != fileToNode.end()) {
                continue;
            }

            
            size_t fileSize = fs::file_size(entry.path());

            
            std::string assignedNode = getLeastLoadedNode();
            fileToNode[filePath] = assignedNode;
            nodeFileSizes[assignedNode] += fileSize;

            
            storeInRedis(filePath, assignedNode);

            std::cout << " File: " << filePath << " (" << fileSize / 1024.0 << " KB)"
                      << " -> Assigned to Node: " << assignedNode << std::endl;

            
            printNodeLoad();
        }
        std::this_thread::sleep_for(std::chrono::seconds(2)); 
    }
}

int main() {
    std::cout << " Watching for new files in directory: " << WATCH_DIR << std::endl;
    processFiles(); 
    return 0;
}

