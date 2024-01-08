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
