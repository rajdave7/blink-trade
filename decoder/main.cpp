#include "tick.h"
#include <boost/asio.hpp>
#include <nlohmann/json.hpp>
#include <iostream>
#include <map>   // ordered container
#include <mutex> // to protect shared data, if needed
#include <librdkafka/rdkafkacpp.h>


const int K = 5; // top 5 levels per side

using boost::asio::ip::udp;
using json = nlohmann::json;

int main()
{
    // bids: highest‑price first. map<price, size>
    std::map<double, int, std::greater<>> bids;

    // asks: lowest‑price first (default). map<price, size>
    std::map<double, int> asks;

    boost::asio::io_context ctx;
    udp::socket socket(ctx);
    // 1) Open socket
    socket.open(udp::v4());
    // 2) Allow multiple listeners on same port
    socket.set_option(boost::asio::ip::udp::socket::reuse_address(true));
    // 3) Bind to port 30001 on any address
    socket.bind(udp::endpoint(udp::v4(), 30001));
    // 4) Join the multicast group
    socket.set_option(
        boost::asio::ip::multicast::join_group(
            boost::asio::ip::make_address("239.255.0.1").to_v4()));

    std::cout << "Listening on 239.255.0.1:30001\n";


    std::string errstr;
    auto conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", "kafka:9092", errstr);
    std::unique_ptr<RdKafka::Producer> producer(
    RdKafka::Producer::create(conf, errstr));
    delete conf;
    
    
    std::unique_ptr<RdKafka::Topic> topic(
    RdKafka::Topic::create(producer.get(), "top_of_book", nullptr, errstr));
    if (!topic) {
        std::cerr << "Failed to create Kafka topic object: " << errstr << "\n";
        return 1;
    }
    
    for (;;)
    {
        char buf[1024];
        udp::endpoint sender;
        size_t len = socket.receive_from(boost::asio::buffer(buf), sender);
        try
        {
            auto j = json::parse(std::string(buf, len));
            Tick t;
            t.symbol = j.at("symbol").get<std::string>();
            t.price = j.at("price").get<double>();
            t.size = j.at("size").get<int>();
            t.timestamp = j.at("timestamp").get<uint64_t>();

            // ask or bid
            std::string s = j.at("side").get<std::string>();
            t.side = s.empty() ? ' ' : s[0];

            // Proof: print it

            if (t.side == 'B')
            {
                if (t.size > 0)
                {
                    // add or update this bid price level
                    bids[t.price] = t.size;
                }
                else
                {
                    // size==0 means delete that level
                    bids.erase(t.price);
                }
                // if we have more than K levels, drop the worst one
                if ((int)bids.size() > K)
                {
                    auto it = std::prev(bids.end()); // worst bid = last element
                    bids.erase(it);
                }
            }
            else if (t.side == 'A')
            {
                if (t.size > 0)
                {
                    asks[t.price] = t.size;
                }
                else
                {
                    asks.erase(t.price);
                }
                if ((int)asks.size() > K)
                {
                    auto it = std::prev(asks.end()); // worst ask = last element
                    asks.erase(it);
                }
            }

            if (!bids.empty() && !asks.empty())
            {
                json top = {
                    {"symbol", t.symbol},
                    {"bestBid", bids.begin()->first},
                    {"bidSize", bids.begin()->second},
                    {"bestAsk", asks.begin()->first},
                    {"askSize", asks.begin()->second},
                    {"timestamp", t.timestamp}};
                
                    std::string payload = top.dump();
                    auto resp = producer->produce(
                    topic.get(),                        // use your Topic*
                    RdKafka::Topic::PARTITION_UA,       // let Kafka pick the partition
                    RdKafka::Producer::RK_MSG_COPY,     // copy the payload
                    const_cast<void*>(static_cast<const void*>(payload.data())),
                    payload.size(),
                    nullptr, nullptr);
                    if (resp != RdKafka::ERR_NO_ERROR)
                    std::cerr << "Kafka produce error: " << RdKafka::err2str(resp) << "\n";
                    producer->poll(0);  // serve delivery callbacks

            }

            std::cout << "[" << t.symbol << "] "
                      << t.price << " @ " << t.timestamp << "\n";
        }
        catch (const nlohmann::json::parse_error &e)
        {
            std::cerr << "JSON parse error at byte " << e.byte << ": " << e.what() << "\n";
            std::cerr << "Raw payload: [" << std::string(buf, len) << "]\n";
        }
        catch (const nlohmann::json::type_error &e)
        {
            std::cerr << "Type error: " << e.what() << "\n";
            std::cerr << "JSON was: " << std::string(buf, len) << "\n";
        }
        catch (const std::exception &e)
        {
            std::cerr << "Other error: " << e.what() << "\n";
        }
    }
}
