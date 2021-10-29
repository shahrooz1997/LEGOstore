#ifndef _Controller_H_
#define _Controller_H_

#include <json.hpp>
#include <fstream>
#include "Data_Transfer.h"
#include "Reconfig.h"
#include <thread>
#include <iostream>

class Controller{
public:
    Controller(uint32_t retry, uint32_t metadata_timeout, uint32_t timeout_per_req, const std::string& detacenters_info_file,
               const std::string& workload_file, const std::string &placements_file);
    Controller(const Controller& orig) = delete;
    virtual ~Controller() = default;
    
    int init_metadata_server();

    int run_clients_for(int group_config_i);
    int run_all_clients();

    int wait_for_clients();

    int run_reconfigurer();

    int warm_up();

private:
    Properties properties;
    unique_ptr<Reconfig> reconfigurer_p;
    std::vector<std::thread> clients_thread;

    int read_detacenters_info(const std::string& file);
    int read_input_workload(const std::string& file);
    int read_placement_one_config(const std::string& file, uint32_t confid);
    int read_placements(const std::string& file);

    int run_client(uint32_t datacenter_id, uint32_t conf_id, uint32_t group_id);
    int kill_datacenter(uint32_t datacenter_id);
};


#endif
