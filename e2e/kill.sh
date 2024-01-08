#!/bin/sh

kill -9 $(lsof -t -i:7680)
kill -9 $(lsof -t -i:7681)
kill -9 $(lsof -t -i:7682)
kill -9 $(lsof -t -i:7683)
kill -9 $(lsof -t -i:7684)
kill -9 $(lsof -t -i:7685)