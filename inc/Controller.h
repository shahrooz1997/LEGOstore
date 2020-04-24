#ifndef _Controller_H_
#define _Controller_H_

#include <json.hpp>
#include <fstream>
#include "Data_Transfer.h"
#include <iostream>

using json = nlohmann::json;

class Controller{

public:
	Controller(uint32_t retry, uint32_t metadata_timeout, uint32_t
							timeout_per_req, std::string setupFile);
	int read_setup_info(std::string &configFile);
	int read_input_workload(std::string &configFile, std::vector<WorkloadConfig*> &input);
	int generate_client_config(const std::vector<WorkloadConfig*> &input);
	int init_setup(std::string configFile);
//TODO:: remove the comment here
//private:
	Properties prp;
};


#endif
