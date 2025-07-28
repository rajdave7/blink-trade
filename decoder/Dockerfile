FROM alpine:3.18
RUN apk add --no-cache g++ make boost-dev curl linux-headers librdkafka-dev
WORKDIR /app
# Vendor JSON
RUN mkdir -p third_party/nlohmann \
    && curl -sL \
    https://raw.githubusercontent.com/nlohmann/json/develop/single_include/nlohmann/json.hpp \
    -o third_party/nlohmann/json.hpp
COPY tick.h main.cpp ./
RUN g++ -std=c++17 main.cpp -o decoder \
    -Ithird_party -lboost_system -lpthread -lrdkafka++ -lrdkafka
CMD ["./decoder"]
