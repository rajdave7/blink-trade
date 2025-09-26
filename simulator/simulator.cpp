#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <thread>
#include <random>
#include <vector>
#include <map>
#include <cstring>
#include <iomanip>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

struct MarketInstrument
{
    std::string symbol;
    double last_price;
    double bid_price;
    double ask_price;
    int bid_size;
    int ask_size;
    double volatility;
    int daily_volume;

    MarketInstrument(const std::string &sym, double price, double vol)
        : symbol(sym), last_price(price), volatility(vol), daily_volume(0)
    {
        // Initialize bid/ask with realistic spread (0.01% - 0.1%)
        double spread_pct = 0.0001 + (vol * 0.001);
        double half_spread = price * spread_pct / 2.0;
        bid_price = price - half_spread;
        ask_price = price + half_spread;
        bid_size = 100 + rand() % 900; // 100-1000 shares
        ask_size = 100 + rand() % 900;
    }
};

class MarketDataSimulator
{
private:
    int sock;
    struct sockaddr_in addr;
    std::vector<MarketInstrument> instruments;
    std::mt19937 rng;
    std::normal_distribution<double> price_dist;
    std::poisson_distribution<int> volume_dist;
    uint64_t sequence_number;

    void initializeInstruments()
    {
        // Major US equities with realistic starting prices
        instruments.emplace_back("AAPL", 175.50, 0.025);
        instruments.emplace_back("MSFT", 338.20, 0.022);
        instruments.emplace_back("GOOGL", 131.40, 0.028);
        instruments.emplace_back("AMZN", 144.80, 0.030);
        instruments.emplace_back("TSLA", 244.16, 0.045);
        instruments.emplace_back("META", 315.30, 0.032);
        instruments.emplace_back("NVDA", 440.25, 0.040);
        instruments.emplace_back("NFLX", 441.85, 0.035);

        // Add some ETFs
        instruments.emplace_back("SPY", 443.20, 0.015);
        instruments.emplace_back("QQQ", 378.45, 0.018);
    }

    std::string generateTickMessage(MarketInstrument &instrument)
    {
        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                             now.time_since_epoch())
                             .count();

        // Random walk with mean reversion
        double price_change = price_dist(rng) * instrument.volatility * instrument.last_price / 100.0;

        // Mean reversion factor (pulls price back to initial value)
        double mean_reversion = -0.001 * price_change;
        price_change += mean_reversion;

        instrument.last_price += price_change;

        // Update bid/ask around new price
        double spread_pct = 0.0001 + (instrument.volatility * 0.001);
        double half_spread = instrument.last_price * spread_pct / 2.0;
        instrument.bid_price = instrument.last_price - half_spread;
        instrument.ask_price = instrument.last_price + half_spread;

        // Occasionally update bid/ask sizes
        if (rand() % 10 == 0)
        {
            instrument.bid_size = 100 + rand() % 900;
            instrument.ask_size = 100 + rand() % 900;
        }

        int trade_size = volume_dist(rng);
        instrument.daily_volume += trade_size;

        // Round prices to realistic precision
        instrument.last_price = round(instrument.last_price * 100.0) / 100.0;
        instrument.bid_price = round(instrument.bid_price * 100.0) / 100.0;
        instrument.ask_price = round(instrument.ask_price * 100.0) / 100.0;

        std::ostringstream json;
        json << "{"
             << "\"msgType\":\"TRADE\","
             << "\"symbol\":\"" << instrument.symbol << "\","
             << "\"price\":" << std::fixed << std::setprecision(2) << instrument.last_price << ","
             << "\"size\":" << trade_size << ","
             << "\"bid\":" << instrument.bid_price << ","
             << "\"ask\":" << instrument.ask_price << ","
             << "\"bidSize\":" << instrument.bid_size << ","
             << "\"askSize\":" << instrument.ask_size << ","
             << "\"timestamp\":" << timestamp << ","
             << "\"sequence\":" << ++sequence_number << ","
             << "\"dailyVolume\":" << instrument.daily_volume
             << "}";

        return json.str();
    }

public:
    MarketDataSimulator() : rng(std::random_device{}()),
                            price_dist(0.0, 1.0),
                            volume_dist(100),
                            sequence_number(0)
    {
        initializeInstruments();

        // Create UDP socket
        sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (sock < 0)
        {
            throw std::runtime_error("Failed to create socket");
        }

        // Set up multicast address
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr("239.255.0.1");
        addr.sin_port = htons(30001);

        std::cout << "Market Data Simulator initialized with " << instruments.size() << " instruments" << std::endl;
        std::cout << "Broadcasting to 239.255.0.1:30001" << std::endl;
    }

    ~MarketDataSimulator()
    {
        if (sock >= 0)
        {
            close(sock);
        }
    }

    void run(int messages_per_second)
    {
        auto interval = std::chrono::microseconds(1000000 / messages_per_second);
        auto next_send_time = std::chrono::steady_clock::now();

        std::cout << "Starting simulation at " << messages_per_second << " msg/s" << std::endl;

        int message_count = 0;
        auto start_time = std::chrono::steady_clock::now();

        while (true)
        {
            // Select random instrument
            auto &instrument = instruments[rand() % instruments.size()];

            // Generate and send message
            std::string message = generateTickMessage(instrument);

            int result = sendto(sock, message.c_str(), message.length(), 0,
                                (struct sockaddr *)&addr, sizeof(addr));

            if (result < 0)
            {
                std::cerr << "Send failed: " << strerror(errno) << std::endl;
                continue;
            }

            message_count++;

            // Print stats every 5000 messages
            if (message_count % 5000 == 0)
            {
                auto now = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
                double actual_rate = message_count / std::max(duration.count(), 1L);

                std::cout << "Sent " << message_count << " messages, "
                          << "Rate: " << actual_rate << " msg/s, "
                          << "Last: " << message.substr(0, 100) << "..." << std::endl;
            }

            // Rate limiting
            next_send_time += interval;
            std::this_thread::sleep_until(next_send_time);
        }
    }
};

int main(int argc, char *argv[])
{
    int rate = 3000; // Default rate
    if (argc > 1)
    {
        rate = std::atoi(argv[1]);
        if (rate <= 0)
            rate = 3000;
    }

    try
    {
        MarketDataSimulator simulator;
        simulator.run(rate);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}