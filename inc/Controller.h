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
	Controller(uint32_t retry, uint32_t metadata_timeout, uint32_t
							timeout_per_req, std::string setupFile);
	int read_setup_info(std::string &configFile);
	int read_input_workload(std::string &configFile, std::vector<WorkloadConfig*> &input);
	int generate_client_config(const std::vector<WorkloadConfig*> &input);
	int init_setup(std::string configFile, std::string filePath);
	int read_deployment_info(std::string &filePath, std::vector<std::pair<std::string, uint16_t> > &info);

	int get_metadata_info(std::string &key, GroupConfig *old_config);
	int update_metadata_info(std::string &key, GroupConfig *new_config);
	Properties prp;

private:
	std::unordered_map<std::string, GroupConfig*> key_metadata;
};


#endif
