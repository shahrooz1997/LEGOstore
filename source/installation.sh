sudo apt-get update
sudo apt install libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev bzip2 build-essential gcc -y
git clone https://github.com/facebook/rocksdb.git /tmp/rocksdb
cd /tmp/rocksdb
git checkout v4.5.1
echo "Compiling RocksDB... This will take a while."
make shared_lib -j 2
sudo make install-shared INSTALL_PATH=/usr
sudo apt install python3-pip
pip3 install pyrocksdb
cd
git clone https://github.com/amitdev/lru-dict.git
cp EC_bacheli/source/lru.c lru-dict/.
cd lru-dict
sudo python3 setup.py install
cd
git clone https://github.com/openstack/pyeclib.git
cd pyeclib
sudo apt install build-essential python-dev python-pip python3-pip liberasurecode-dev -y
sudo pip3 install -U bindep -r test-requirements.txt
sudo python3 setup.py install
sudo sh -c "echo '/usr/local/lib' >> /etc/ld.so.conf"
sudo ldconfig
sudo pip3 install numpy
sudo pip3 install python3-logstash
