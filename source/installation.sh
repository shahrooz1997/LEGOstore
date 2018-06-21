sudo apt-get update
sudo apt install libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev bzip2 build-essential gcc -y
git clone https://github.com/facebook/rocksdb.git /tmp/rocksdb
cd /tmp/rocksdb
sudo make install-shared INSTALL_PATH=/usr
sudo apt install python3-pip
pip3 install pyrocksdb
cd
git clone https://github.com/amitdev/lru-dict.git
cp erasurecodingproject/source/lru.c lru-dict/.
cd lru-dict
sudo python3 setup.py install
cd
git clone https://github.com/openstack/pyeclib.git
cd pyeclib
sudo apt-get install build-essential python-dev python-pip liberasurecode-dev
sudo pip install -U bindep -r test-requirements.txt
sudo python3 setup.py install
sudo echo ‘include /usr/local/lib’ >> /etc/ld.so.conf
sudo ldconfig
