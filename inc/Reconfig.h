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

    int update_metadata_info(std::string& key, uint32_t old_confid_id, uint32_t new_confid_id, const std::string& timestamp,
                             const Placement& p);
    int update_metadata_state(std::string& key, uint32_t old_confid_id, std::string& op, const std::string& timestamp);
private:
    
    int send_reconfig_query(GroupConfig& old_config, uint32_t old_conf_id, const std::string& key, Timestamp*& ret_ts,
            std::string& ret_v);
    
    int send_reconfig_finalize(GroupConfig& old_config, uint32_t old_conf_id, const std::string& key, Timestamp* const & ts,
                               std::string& ret_v);
    
    int send_reconfig_write(GroupConfig& new_config, uint32_t new_conf_id, const std::string& key, Timestamp* const & ret_ts,
                            const std::string& ret_v);
    
    int
    send_reconfig_finish(GroupConfig& old_config, uint32_t old_conf_id, uint32_t new_conf_id, const std::string& key,
                         Timestamp* const & ts);
    
//    std::unordered_map<std::string, GroupConfig*> key_metadata;
//    std::mutex lock_t;

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
