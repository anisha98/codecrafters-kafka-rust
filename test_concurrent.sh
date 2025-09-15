#!/bin/bash

echo "Testing concurrent connections..."

# Send 3 requests simultaneously in the background
(echo -n "00000023001200040000000100096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 > /dev/null) &
(echo -n "00000023001200040000000200096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 > /dev/null) &
(echo -n "00000023001200040000000300096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 > /dev/null) &

# Wait for all background jobs to complete
wait

echo "All concurrent requests completed!"
