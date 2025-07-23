#pragma once
#include <string>
struct Tick
{
    std::string symbol;
    double price;
    int size;
    uint64_t timestamp;
};