#!/usr/bin/env bash
  
sudo sysctl net.core.somaxconn=$1 > /dev/null
ulimit -n $2
cd ~/LEGOstore/
make server > /dev/null
./Server $3 $4
