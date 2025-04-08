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


size_t computeFileHash(const std::string& filename) {
    boost::hash<std::string> hasher;
    return hasher(filename) % REDIS_NODES.size(); 
}

// Connect to a Redis node
redisContext* connectToRedis(const std::string& redisAddress) {
    size_t colonPos = redisAddress.find(":");
    std::string host = redisAddress.substr(0, colonPos);
    int port = std::stoi(redisAddress.substr(colonPos + 1));

    redisContext* context = redisConnect(host.c_str(), port);
    if (context == nullptr || context->err) {
        std::cerr << "Error: Unable to connect to Redis at " << redisAddress << std::endl;
        return nullptr;
    }
    return context;
}

// Read file content
std::string readFileContent(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file) {
        std::cerr << "Error: Unable to open file " << filePath << std::endl;
        return "";
    }
    return std::string((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
}

// Store file content in Redis
void storeFileInRedis(const std::string& filename, const std::string& fileContent, const std::string& redisNode) {
    redisContext* context = connectToRedis(redisNode);
    if (!context) return;

    redisReply* reply = (redisReply*)redisCommand(context, "SET %s %b", filename.c_str(), fileContent.c_str(), fileContent.size());
    if (reply) {
        std::cout << "Stored File: " << filename << " -> Redis Node: " << redisNode << std::endl;
        freeReplyObject(reply);
    } else {
        std::cerr << "Error storing file in Redis." << std::endl;
    }

    redisFree(context);
}

// Assign file to Redis node and store its content
void assignFileToRedis(const std::string& filePath) {
    std::string filename = fs::path(filePath).filename().string(); 
    size_t nodeIndex = computeFileHash(filename);
    std::string redisNode = REDIS_NODES[nodeIndex];

    std::string fileContent = readFileContent(filePath);
    if (fileContent.empty()) return; 

    storeFileInRedis(filename, fileContent, redisNode);
}

// Monitor directory and process new files
void processFiles() {
    while (true) {
        for (const auto& entry : fs::directory_iterator(WATCH_DIR)) {
            std::string filePath = entry.path().string();
            std::string filename = fs::path(filePath).filename().string(); 

            
            if (!fs::is_regular_file(entry) || fileToNode.find(filename) != fileToNode.end()) {
                continue;
            }

           
            assignFileToRedis(filePath);

            
            fileToNode[filename] = "processed";
        }
        std::this_thread::sleep_for(std::chrono::seconds(20)); 
    }
}

int main() {
    std::cout << "Watching for new files in directory: " << WATCH_DIR << std::endl;
    processFiles(); // Start processing files
    return 0;
}

