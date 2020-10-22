/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   Reconfig.h
 * Author: shahrooz
 *
 * Created on March 23, 2020, 6:34 PM
 */

#ifndef RECONFIG_H
#define RECONFIG_H

#include "Util.h"
#include <stdint.h>
#include <string>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "Timestamp.h"
#include <erasurecode.h>
#include "Data_Transfer.h"

class Reconfig{
public:

    Reconfig(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
            uint32_t timeout_per_request, std::vector<DC*>& datacenters);

    Reconfig(const Reconfig& orig) = delete;

    virtual ~Reconfig();
    
    int start_reconfig(GroupConfig& old_config, uint32_t old_conf_id, GroupConfig& new_config, uint32_t new_conf_id);
    
//    int get_metadata_info(std::string& key, GroupConfig** old_config);
    
    int update_metadata_info(std::string& key, uint32_t old_confid, uint32_t new_confid, const std::string& timestamp,
                             const Placement& p);

private:
    
    int send_reconfig_query(GroupConfig& old_config, uint32_t old_conf_id, const std::string& key, Timestamp*& ret_ts,
            std::string& ret_v);
    
    int send_reconfig_finalize(Properties* prop, GroupConfig& old_config, std::string& key, Timestamp* ret_ts,
            std::string& ret_v, int desc);
    
    int send_reconfig_write(Properties* prop, GroupConfig& new_config, std::string& key, Timestamp* ret_ts,
            std::string& ret_v, int desc);
    
    int
    send_reconfig_finish(Properties* prop, GroupConfig& old_config, GroupConfig& new_config, std::string& key,
            Timestamp* ret_ts);
    
    std::unordered_map<std::string, GroupConfig*> key_metadata;
    std::mutex lock_t;


    uint32_t id;
    uint32_t local_datacenter_id;
    uint32_t retry_attempts;
    uint32_t metadata_server_timeout;
    uint32_t timeout_per_request;
    std::vector<DC*>& datacenters; // Should not be deleted in Client destructor
    std::string metadata_server_ip;
    std::string metadata_server_port;

};

#endif /* RECONFIG_H */
