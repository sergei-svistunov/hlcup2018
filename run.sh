#!/usr/bin/env bash

sed -e '/^$/,$d' /proc/cpuinfo

cd /root/build

if make VERBOSE=1 >/tmp/make.log 2>&1; then
	./hlcup2018 80 /tmp/data/data.zip
else
	cat /tmp/make.log
fi