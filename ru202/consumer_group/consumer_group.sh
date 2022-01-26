#!/bin/bash

# This is a lab, so everything is built with the debug profile.

# If the main directory exists, build consumer_group_main
if [[ -d ./main ]]; then
    echo "[>] Building consumer_group_main..."
    cd main
    cargo build || exit
    cd ..
    echo -e "\n\n"
fi

# If the consumer directory exists, build consumer_group_consumer
if [[ -d ./consumer ]]; then
    echo "[>] Building consumer_group_consumer..."
    cd consumer
    cargo build || exit
    cd ..
    echo -e "\n\n"
fi

# If the bin directory does not exist, create it.
if [[ ! -d ./bin ]]; then
    mkdir bin
fi

# If the bin directory exists
# and if the executables exist,
# copy the executables into bin.
MAIN="./main/target/debug/consumer_group_main"
CONSUMER="./consumer/target/debug/consumer_group_consumer"

if [[ -f "$MAIN" ]]; then
    cp -v "$MAIN" ./bin/
fi

if [[ -f "$CONSUMER" ]]; then
    cp -v "$CONSUMER" ./bin/
fi

# Run the lab by executing consumer_group_main
echo -e "\n\n#### Consumer Group Lab ###\n\n"
cd bin
./consumer_group_main
