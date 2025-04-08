#include <iostream>
#include <unordered_map>
#include <fstream>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class KeyValueNode {
private:
    int port;
    std::unordered_map<int, std::string> store;
    std::string filename;

    void loadFromFile() {
        std::ifstream file(filename);
        if (file.is_open()) {
            int key;
            std::string value;
            while (file >> key) {
                std::getline(file, value);
                if (!value.empty() && value[0] == ' ') {
                    value = value.substr(1);
                }
                store[key] = value;
            }
            file.close();
            std::cout << "[PORT " << port << "] Loaded " << store.size() << " keys from file.\n";
        }
    }

    void saveToFile(int key, const std::string &value) {
        std::ofstream file(filename, std::ios::app);
        if (file.is_open()) {
            file << key << " " << value << "\n";
            file.close();
        }
    }

    void startServer() {
        try {
            boost::asio::io_context io_context;
            tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), port));

            std::cout << "Node running on port " << port << std::endl;

            while (true) {
                tcp::socket socket(io_context);
                acceptor.accept(socket);

                boost::asio::streambuf buffer;
                boost::asio::read_until(socket, buffer, "\n");
                std::istream is(&buffer);
                int key;
                std::string value;
                is >> key;
                std::getline(is, value);

                if (!value.empty() && value[0] == ' ') {
                    value = value.substr(1);
                }

                std::string response;
                if (store.find(key) != store.end()) {
                    response = "Error: Key " + std::to_string(key) + " already exists!\n";
                    std::cout << "[PORT " << port << "] Duplicate key attempt: " << key << "\n";
                } else {
                    store[key] = value;
                    saveToFile(key, value);
                    response = "Stored: " + std::to_string(key) + " -> " + value + "\n";
                    std::cout << "[PORT " << port << "] Stored: " << key << " -> " << value << std::endl;
                }

                boost::asio::write(socket, boost::asio::buffer(response));
            }
        } catch (std::exception &e) {
            std::cerr << "Error on node " << port << ": " << e.what() << std::endl;
        }
    }

public:
    KeyValueNode(int p) : port(p) {
        filename = "node_" + std::to_string(port) + ".txt";
        loadFromFile();
    }

    void run() {
        startServer();
    }
};

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: ./node <port_number>\n";
        return 1;
    }

    int port = std::stoi(argv[1]);
    KeyValueNode node(port);
    node.run();

    return 0;
}

