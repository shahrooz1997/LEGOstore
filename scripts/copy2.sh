#!/bin/bash

rm -rf project
rm -rf project.tar.gz
mkdir project
cp -r ../inc ./project/
# cp -r ../lib ./project/
cp -r ../src ./project/
cp ../Makefile ./project/

tar -czf project.tar.gz project/
