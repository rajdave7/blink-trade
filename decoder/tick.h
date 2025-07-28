#pragma once
#include <string>
#include <cstdint>

struct Tick {
  std::string symbol;    // e.g. "AAPL"
  double      price;     // e.g. 123.45
  int         size;      // e.g. 100
  uint64_t    timestamp; // ms since epoch
  char        side;      // 'B' for bid, 'A' for ask
};
