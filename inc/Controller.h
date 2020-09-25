#ifndef _Controller_H_
#define _Controller_H_

#include <json.hpp>
#include <fstream>
#include "Data_Transfer.h"
#include "Reconfig.h"
#include <thread>
#include <iostream>

using json = nlohmann::json;

class Controller{

public:
    Controller(uint32_t retry, uint32_t metadata_timeout, uint32_t timeout_per_req, std::string setupFile);
    
    int read_detacenters_info(std::string& configFile);
    
    int read_input_workload(const std::string& configFile, std::vector<WorkloadConfig*>& input);
    
    int generate_client_config(const std::vector<WorkloadConfig*>& input);
    
    int init_setup(const std::string& configFile);
    
//    int read_deployment_info(std::string& filePath, std::vector <std::pair<std::string, uint16_t>>& info);
    
//    int send_config_group_to_client(uint32_t group_number);
    
    int init_metadata_server();
    
    int run_client(uint32_t datacenter_id, uint32_t conf_id, uint32_t group_id);
    
    Properties prp;
    Reconfig config_t;
private:

};


#endif
