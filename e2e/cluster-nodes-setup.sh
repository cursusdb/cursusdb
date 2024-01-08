#!/bin/sh
#kill -9 $(lsof -t -i:7680)
kill -9 $(lsof -t -i:7681)
kill -9 $(lsof -t -i:7682)
kill -9 $(lsof -t -i:7683)
kill -9 $(lsof -t -i:7684)
kill -9 $(lsof -t -i:7685)

cd cluster
(sleep 1; echo "test"; sleep 1; echo "password"; sleep 1; echo "sharedkey" ) | ./cursus

cd ../node1
echo "sharedkey" | ./curode &
pid=$! # set last run command's process id into pid var
sleep 1s
kill -9 $pid # kill that pid

cd ../node2
echo "sharedkey" | ./curode &
pid=$! # set last run command's process id into pid var
sleep 1s
kill -9 $pid # kill that pid

cd ../node1replica
echo "sharedkey" | ./curode &
pid=$! # set last run command's process id into pid var
sleep 1s
kill -9 $pid # kill that pid

cd ../node2replica
echo "sharedkey" | ./curode &
pid=$! # set last run command's process id into pid var
sleep 1s
kill -9 $pid # kill that pid