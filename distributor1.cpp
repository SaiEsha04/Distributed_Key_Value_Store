#include <iostream>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <boost/functional/hash.hpp>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class KeyDistributor {
private:
    int num_nodes;
    std::vector<int> node_ports;
    std::unordered_map<int, int> key_to_node; // Stores known key-node mappings

    void loadMappings() {
        std::ifstream file("mappings.txt");
        if (file.is_open()) {
            int key, node_idx, port;
            std::string node_name;
            while (file >> key >> node_name >> node_idx >> port) {
                key_to_node[key] = node_idx;
            }
            file.close();
        }
    }

    void saveMapping(int key, int node_idx) {
        std::ofstream file("mappings.txt", std::ios::app);
        if (file.is_open()) {
            file << key << " Node " << node_idx << " " << node_ports[node_idx] << "\n";
            file.close();
        } else {
            std::cerr << "Error: Unable to open file for saving mappings.\n";
        }
    }

    int getNodeIndex(int key) {
        boost::hash<int> int_hash;
        return int_hash(key) % num_nodes;
    }

    bool sendToNode(int node_idx, int key, const std::string &value) {
        try {
            boost::asio::io_context io_context;
            tcp::socket socket(io_context);
            tcp::resolver resolver(io_context);
            tcp::resolver::results_type endpoints = resolver.resolve("127.0.0.1", std::to_string(node_ports[node_idx]));
            boost::asio::connect(socket, endpoints);

            std::string message = std::to_string(key) + " " + value + "\n";
            boost::asio::write(socket, boost::asio::buffer(message));

            boost::asio::streambuf response;
            boost::asio::read_until(socket, response, "\n");
            std::istream is(&response);
            std::string server_response;
            std::getline(is, server_response);

            if (server_response.find("Error:") != std::string::npos) {
                std::cerr << "Node Error: " << server_response << std::endl;
                return false;
            }
            return true;
        } catch (std::exception &e) {
            std::cerr << "Failed to send data to node " << node_idx << ": " << e.what() << std::endl;
            return false;
        }
    }

public:
    KeyDistributor(int n, std::vector<int> ports) : num_nodes(n), node_ports(std::move(ports)) {
        loadMappings();
    }

    void put(int key, const std::string &value) {
        if (key_to_node.find(key) != key_to_node.end()) {
            std::cerr << "Error: Key " << key << " already exists and was assigned to Node " << key_to_node[key] << "\n";
            return;
        }

        int node_idx = getNodeIndex(key);
        if (sendToNode(node_idx, key, value)) {
            key_to_node[key] = node_idx;
            saveMapping(key, node_idx);
            std::cout << "Stored: " << key << " -> Node " << node_idx << " (Port " << node_ports[node_idx] << ")\n";
        }
    }
};

int main() {
    std::vector<int> ports = {5000, 5001, 5002};
    KeyDistributor distributor(ports.size(), ports);

    std::cout << "Enter key-value pairs (integer key, string value). Type 'exit' to stop.\n";

    while (true) {
        std::string input;
        std::cout << "Enter key and value: ";
        std::getline(std::cin, input);

        if (input == "exit") break;

        size_t space_pos = input.find(' ');
        if (space_pos == std::string::npos) {
            std::cout << "Invalid input. Use: <key> <value>\n";
            continue;
        }

        try {
            int key = std::stoi(input.substr(0, space_pos));
            std::string value = input.substr(space_pos + 1);
            distributor.put(key, value);
        } catch (...) {
            std::cout << "Invalid key format. Key should be an integer.\n";
        }
    }

    std::cout << "Exiting...\n";
    return 0;
}

