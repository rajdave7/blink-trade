// simulator.cpp (build with: g++ -std=c++17 simulator.cpp -o simulator)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <iostream>
#include <string>
#include <random>
#include <sstream>
#include <nlohmann/json.hpp> // optional: but we'll produce manual JSON to avoid dependency.

using namespace std;
int main(int argc, char **argv)
{
    string mcast_addr = "239.255.0.1";
    int port = 30001;
    int rate = 3000; // messages per second
    if (argc > 1)
        rate = atoi(argv[1]);
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0)
    {
        perror("socket");
        return 1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, mcast_addr.c_str(), &addr.sin_addr);

    // TTL
    int ttl = 1;
    setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

    std::default_random_engine gen((unsigned)time(nullptr));
    std::uniform_real_distribution<double> px(100.0, 200.0);
    std::uniform_int_distribution<int> size(1, 1000);
    vector<string> syms = {"AAPL", "MSFT", "GOOG", "TSLA", "AMZN"};
    int sym_count = syms.size();

    using clock = std::chrono::high_resolution_clock;
    double interval_ms = 1000.0 / rate;
    while (true)
    {
        // Build JSON string manually
        double p = px(gen);
        int s = size(gen);
        string symbol = syms[rand() % sym_count];
        auto now = chrono::duration_cast<chrono::microseconds>(clock::now().time_since_epoch()).count();
        double ts = now / 1e6;
        std::ostringstream ss;
        ss << "{\"ts\":" << fixed << ts << ",\"sym\":\"" << symbol << "\",\"px\":" << p << ",\"size\":" << s << ",\"type\":\"trade\"}";
        string msg = ss.str();
        sendto(sock, msg.c_str(), msg.size(), 0, (sockaddr *)&addr, sizeof(addr));
        std::this_thread::sleep_for(std::chrono::microseconds((int)interval_ms * 1000));
    }
    close(sock);
    return 0;
}
