#!/bin/#!/usr/bin/env bash
sudo apt install openjdk-8-jre-headless
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
sudo apt-get install apt-transport-https
echo "deb https://artifacts.elastic.co/packages/6.x/apt stable main" | sudo tee -a /etc/apt/sources.list.d/elastic-6.x.list
sudo apt-get update
sudo apt-get install logstash
sudo systemctl start logstash.service
sudo  /usr/share/logstash/bin/logstash -f logstash.conf
