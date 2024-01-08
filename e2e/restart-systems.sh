#!/bin/sh

cd ./node1
./curode & echo $!
sleep 1s
cd ../node2
./curode & echo $!
sleep 1s
cd ../node1replica
./curode & echo $!
sleep 1s
cd ../node2replica
./curode & echo $!
sleep 1s
cd ../cluster
./cursus & echo $!

# Currently it is best to have the observer running already!!

## start observer
#kill -9 $(lsof -t -i:7680)
#cd ../observer
#npm install
#node main.js &
## observer to log writes to file so test can check for receival