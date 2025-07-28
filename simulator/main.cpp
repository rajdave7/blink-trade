#include "tick.h"

#include <boost/asio.hpp>
#include <nlohmann/json.hpp>
#include <thread>
#include <vector>
#include <chrono>
#include <iostream> // <<— for debug prints
#include <cstdlib>  // <<— for rand()
#include <ctime>    // <<— for time()

using boost::asio::ip::udp;
using json = nlohmann::json;

void send_ticks(const std::string &symbol,
                udp::endpoint endpoint,
                int rate)
{
    // seed per‐thread so you get varying prices
    std::srand(static_cast<unsigned>(std::time(nullptr)));

    boost::asio::io_context ctx;
    udp::socket socket(ctx);
    socket.open(udp::v4());

    std::cout << "Thread for " << symbol
              << " sending to "
              << endpoint.address().to_string()
              << ":" << endpoint.port() << "\n";

    Tick t;
    t.symbol = symbol;
    t.size = 100;

    while (true)
    {
        t.price = 100.0 + (std::rand() % 1000) / 10.0;
        t.timestamp = std::chrono::duration_cast<
                          std::chrono::milliseconds>(
                          std::chrono::system_clock::now()
                              .time_since_epoch())
                          .count();
        t.side = (rand() % 2 == 0 ? 'B' : 'A');

        json j = {
            {"symbol",    t.symbol},
            {"price",     t.price},
            {"size",      t.size},
            {"timestamp", t.timestamp},
            {"side",      std::string(1, t.side)}
        };                        
        

        auto payload = j.dump() + "\n";
        socket.send_to(boost::asio::buffer(payload), endpoint);

        // heartbeat every 1000 messages so you can see it
        static int counter = 0;
        if (++counter % 1000 == 0)
            std::cout << symbol
                      << " sent 1000 ticks. latest price="
                      << t.price << "\n";

        std::this_thread::sleep_for(
            std::chrono::milliseconds(1'000'000 / rate));
    }
}

int main()
{
    const std::vector<std::string> symbols = {
        "AAPL", "GOOG", "MSFT"};

    // STILL using multicast address here:
    udp::endpoint endpoint(
        boost::asio::ip::make_address("239.255.0.1"),
        30001);

    int rate = 1000; // ticks per second per symbol

    std::vector<std::thread> threads;
    threads.reserve(symbols.size());

    for (auto &symbol : symbols)
    {
        std::cout << "[" << symbol << "] starting sender loop\n";

        threads.emplace_back(send_ticks, symbol, endpoint, rate);

        static int count = 0;

        if (++count % 100 == 0)
            std::cout << "[" << symbol << "] sent " << count << " ticks\n";
    }

    for (auto &t : threads)
        t.join();

    return 0;
}
