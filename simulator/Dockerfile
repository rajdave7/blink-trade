# Dockerfile
FROM alpine:3.18

# 1) Install compiler, Boost, curl
RUN apk add --no-cache \
    g++ make       \
    boost-dev      \
    curl           \
    linux-headers

WORKDIR /app

# 2) Vendor the nlohmann header so #include <nlohmann/json.hpp> still works
RUN mkdir -p third_party/nlohmann \
    && curl -sL \
    https://raw.githubusercontent.com/nlohmann/json/develop/single_include/nlohmann/json.hpp \
    -o third_party/nlohmann/json.hpp

# 3) Copy your code in
COPY . .

# 4) Build, telling g++ where to find our vendored header
RUN g++ -std=c++17 main.cpp -o simulator \
    -Ithird_party       \
    -lboost_system      \
    -lpthread

CMD ["./simulator"]
