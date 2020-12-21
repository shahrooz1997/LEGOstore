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

#include "Client.h"
#include "Liberasure.h"

class Reconfig : public Client{
public:

    Reconfig(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
            uint32_t timeout_per_request, std::vector<DC*>& datacenters);
    Reconfig(const Reconfig& orig) = delete;
    virtual ~Reconfig();
    
    int reconfig(GroupConfig& old_config, uint32_t old_conf_id, GroupConfig& new_config, uint32_t new_conf_id);

    int put(const std::string& key, const std::string& value);
    int get(const std::string& key, std::string& value);

private:
    Liberasure liberasure;

    int send_reconfig_query(GroupConfig& old_config, uint32_t old_conf_id, const std::string& key, unique_ptr<Timestamp>& ts,
            std::string& ret_v);
    int send_reconfig_finalize(GroupConfig& old_config, uint32_t old_conf_id, const std::string& key, unique_ptr<Timestamp>& ts,
                               std::string& ret_v);
    int send_reconfig_write(GroupConfig& new_config, uint32_t new_conf_id, const std::string& key, unique_ptr<Timestamp>& ret_ts,
                            const std::string& ret_v);
    int update_metadata_info(std::string& key, uint32_t old_confid_id, uint32_t new_confid_id, const std::string& timestamp,
                             const Placement& p);
    int send_reconfig_finish(GroupConfig& old_config, uint32_t old_conf_id, uint32_t new_conf_id, const std::string& key,
                             unique_ptr<Timestamp>& ts);
};

#endif /* RECONFIG_H */
