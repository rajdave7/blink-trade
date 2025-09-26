
FROM gcc:12
WORKDIR /app

# Install nlohmann/json library
RUN apt-get update && apt-get install -y \
    nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY simulator.cpp /app/simulator.cpp
RUN g++ -std=c++17 simulator.cpp -O2 -o /app/simulator
CMD ["/app/simulator", "3000"]