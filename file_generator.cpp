#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <filesystem>

namespace fs = std::filesystem;
const std::string TARGET_DIR = "."; 

void generateFiles() {
    int fileCount = 1;
    while (true) {
        
        std::string fileName = TARGET_DIR + "/file" + std::to_string(fileCount) + ".txt";
        
        
        std::ofstream file(fileName);
        if (file) {
            file << "THIS IS FILE " << fileCount << ".TXT" << std::endl; 
            std::cout << "Created: " << fileName << std::endl;
        }
        file.close();
        
        fileCount++; 

        
        std::this_thread::sleep_for(std::chrono::seconds(30));
    }
}

int main() {
    generateFiles();
    return 0;
}

