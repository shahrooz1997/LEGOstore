[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

---------------------------------------

# LEGOStore
LEGOStore is a linearizable geo-distributed key/value store, combining replication and erasure coding. LEGOStore's main
focus is to minimize cost considering workload characteristics and public cloud price structures.

Also, to handle workload dynamism, LEGOStore employs a novel agile reconfiguration protocol.

This project consists of two main projects:
* Optimizer: it computes the cost-optimized placement of objects from your workload.
* LEGOStore prototype: it simulates your workload using the output of the optimizer.

# Run the optimizer
Define your workload in `config/auto_test/input_workload.json` with the following structure.
```json
{
  "workload_config": [
    {
      "timestamp": 22,
      "id": 1,
      "grp_workload": [
        {
          "availability_target": 1,
          "client_dist": [
            0.2,
            0.2,
            0.2,
            0.2,
            0.2,
            0,
            0,
            0,
            0
          ],
          "object_size": 81.0,
          "metadata_size": 1e-09,
          "num_objects": 1,
          "arrival_rate": 20.1678,
          "read_ratio": 0.99999,
          "write_ratio": 1e-05,
          "SLO_read": 1000,
          "SLO_write": 1000,
          "duration": 600,
          "time_to_decode": 0.00028,
          "id": 0,
          "keys": [
            "0"
          ]
        }
      ]
    },
    {
      "timestamp": 622,
      "id": 2,
      "grp_workload": [
        {
          "availability_target": 1,
          "client_dist": [
            0.1111111111111111,
            0.1111111111111111,
            0.1111111111111111,
            0.1111111111111111,
            0.1111111111111111,
            0.1111111111111111,
            0.1111111111111111,
            0.1111111111111111,
            0.1111111111111111
          ],
          "object_size": 81.0,
          "metadata_size": 1e-09,
          "num_objects": 1,
          "arrival_rate": 49.5528,
          "read_ratio": 0.99999,
          "write_ratio": 1e-05,
          "SLO_read": 1000,
          "SLO_write": 1000,
          "duration": 600,
          "time_to_decode": 0.00028,
          "id": 0,
          "keys": [
            "0"
          ]
        }
      ]
    }
  ]
}
```
Each workload can have many `workload_config` where the config for one group of keys can change. Each `workload_config`
has a timestamp that shows how many seconds after the simulator has started that `workload_config` should start. The
`id` should increase monotonically. Each `workload_config` can have several group of keys with the same workload
characteristics.

You will also need to set the pricing structure in `optimizer/tests/inputtests/dc_gcp.json`.

Then you can run the optimizer, by going to the `scripts` directory, and run `./run_optimizer.py`. It will create the
optimizer output files (`optimizer_output_<workload_config_id>.json`, one for each `workload_config`) that has the best
placement for each group of keys in that `workload_config`. E.g.: (the values in the example might not be accurate)
```json
{
  "output": [
    {
      "id": 0,
      "protocol": "abd",
      "m": 3,
      "selected_dcs": [
        1,
        3,
        4
      ],
      "get_cost": 0.0011996978208218209,
      "put_cost": 6.0726052512e-09,
      "storage_cost": 1.6643835616438355e-11,
      "vm_cost": 0.00354519119567828,
      "total_cost": 0.004744895105749188,
      "get_latency": 554,
      "put_latency": 554,
      "client_placement_info": {
        "0": {
          "Q1": [
            3,
            4
          ],
          "Q2": [
            3,
            4
          ],
          "get_cost_on_this_client": 2.592e-08,
          "put_cost_on_this_client": 1.312e-08,
          "get_latency_on_this_client": 452.2,
          "put_latency_on_this_client": 452.2
        },
        "1": {
          "Q1": [
            1,
            4
          ],
          "Q2": [
            1,
            4
          ],
          "get_cost_on_this_client": 2.43e-08,
          "put_cost_on_this_client": 1.23e-08,
          "get_latency_on_this_client": 554,
          "put_latency_on_this_client": 554
        },
        "2": {
          "Q1": [
            3,
            4
          ],
          "Q2": [
            3,
            4
          ],
          "get_cost_on_this_client": 2.592e-08,
          "put_cost_on_this_client": 1.312e-08,
          "get_latency_on_this_client": 406.6,
          "put_latency_on_this_client": 406.6
        },
        "3": {
          "Q1": [
            3,
            4
          ],
          "Q2": [
            3,
            4
          ],
          "get_cost_on_this_client": 3.24e-09,
          "put_cost_on_this_client": 1.64e-09,
          "get_latency_on_this_client": 29.58,
          "put_latency_on_this_client": 29.58
        },
        "4": {
          "Q1": [
            3,
            4
          ],
          "Q2": [
            3,
            4
          ],
          "get_cost_on_this_client": 3.24e-09,
          "put_cost_on_this_client": 1.64e-09,
          "get_latency_on_this_client": 30.42,
          "put_latency_on_this_client": 30.42
        },
        "5": {
          "Q1": [
            3,
            4
          ],
          "Q2": [
            3,
            4
          ],
          "get_cost_on_this_client": 2.592e-08,
          "put_cost_on_this_client": 1.312e-08,
          "get_latency_on_this_client": 179.4,
          "put_latency_on_this_client": 179.4
        },
        "6": {
          "Q1": [
            3,
            4
          ],
          "Q2": [
            3,
            4
          ],
          "get_cost_on_this_client": 2.592e-08,
          "put_cost_on_this_client": 1.312e-08,
          "get_latency_on_this_client": 404.2,
          "put_latency_on_this_client": 404.2
        },
        "7": {
          "Q1": [
            3,
            4
          ],
          "Q2": [
            3,
            4
          ],
          "get_cost_on_this_client": 2.592e-08,
          "put_cost_on_this_client": 1.312e-08,
          "get_latency_on_this_client": 306.8,
          "put_latency_on_this_client": 306.8
        },
        "8": {
          "Q1": [
            3,
            4
          ],
          "Q2": [
            3,
            4
          ],
          "get_cost_on_this_client": 2.592e-08,
          "put_cost_on_this_client": 1.312e-08,
          "get_latency_on_this_client": 283.6,
          "put_latency_on_this_client": 283.6
        }
      }
    }
  ],
  "overall_cost": 1.4054225842789652,
  "get_cost": 0.9946701490111117,
  "put_cost": 4.03269158499279e-05,
  "storage_cost": 2.058005750684933e-06,
  "vm_cost": 0.4107100503462571,
  "time_period_hrs": 1550.0
}
```
It shows the best placement for each group of keys and which nodes the client in each region should access to do
operations on those keys. It also includes the cost if the workload follows the same characteristics as specified in the
input.

# Run the prototype (simulator)
To run the simulator, you can use the `scripts/run.py` script. It uses `gcloud` under the hood to provision all the
required nodes on GCP for simulation and install all the prerequisites on those nodes and run the LEGOStore servers.
It then runs the clients to do put/get operations based on your workload specification in
`config/auto_test/input_workload.json` file. Please note that the `Metadata_Server` will be initialized based on the
output of the optimizer so each key will have the optimized placement.

The `scripts/run.py` script will gather the logs at the end of the simulation and put them in `scripts/data/CAS_NOF`
directory for interpretation.

All the artifacts used in the paper was created with the methods described above and then we used Python scripts to read
the artifacts and used `matplotlib` to draw the figures in the paper. 

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

# Read More
For more details about this work, please refer to the paper.
