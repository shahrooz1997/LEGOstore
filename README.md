# Erasure Coding based System
Aim of this system is to provide the infra-structure for implementing easure coding based protocols for different consistencies in distributed systems. This is a wrapper around mcrouter library for supporting erasure coding using memcached as cache and mysql as backend.

# Config File
There is config.json file which will be used to configure our customised router.

1. quorum : Defines the default quoram properties for each request unless specified by each request.
    1. "total_nodes": Defines the number of servers for each request. Make sure it is less than or equals to total number of servers you have.
    2. "read_nodes": Number of nodes we should read each request. 
    3. "write_nodes": Number of nodes we should write each request. 
    4. "dimensions": Dimensions for the erasure coding parameter
2. datacenters : Defines the list of datacenters. Remember each datacenter has its own mcrouter and here we provide the host and port for particular mcrouter for that datacenter.
    1. Each datacenter will have :
        a. host: Address of the mcrouter for the host
        b. port: Port on which its running
        c. id : Unique id associated with that datacenter. 

# Install
Install the prerequisites:
```commandline
sudo apt-get install -y libprotobuf-dev protobuf-compiler build-essential autoconf automake libtool \
    zlib1g-dev git protobuf-compiler pkg-config psmisc bc aria2 libgflags-dev cmake librocksdb-dev
```

Then you need to install liberasurecode:
```commandline
git clone https://github.com/openstack/liberasurecode.git
cd liberasurecode/
./autogen.sh 
./configure --prefix=/usr
make -j9
sudo make install
```

Then you need to increase the limits of opening files per application so you can have persistent sockets.
```commandline
sudo bash -c 'printf "* hard nofile 97816\n* soft nofile 97816\n" >> /etc/security/limits.conf'
```