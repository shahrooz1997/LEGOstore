/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   CAS_Client.h
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:35 PM
 */

/*
    #########################
    # Quorum relevant properties
    # Q1 + Q3 > N
    # Q1 + Q4 > N
    # Q2 + Q4 >= N+k
    # Q4 >= k
    #########################
*/


#ifndef CAS_Client_H
#define CAS_Client_H

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
#include <map>
#include <utility>
#include <sys/time.h>
#include <future>

class CAS_Client {
public:
//    CAS_Client(uint32_t client_id, uint32_t local_datacenter_id, std::vector <DC*> &datacenters, std::map<std::string, std::pair<uint32_t, Placement> > *keys_info);
    CAS_Client(uint32_t client_id, uint32_t local_datacenter_id, std::vector <DC*> &datacenters, int *desc_l, std::map<std::string, std::pair<uint32_t, Placement> > *keys_info);
    CAS_Client(const CAS_Client& orig) = delete;
    virtual ~CAS_Client();

    int put(std::string key, std::string value, bool insert = false);
    int get(std::string key, std::string &value);

private:
    
    uint32_t                local_datacenter_id;
    uint32_t                retry_attempts;
    uint32_t                metadata_server_timeout;
    uint32_t                timeout_per_request;
    uint64_t                start_time;
    std::vector <DC*>       datacenters; // Should not be deleted in Client destructor

    uint32_t id;
//    Properties *prop;
//    Placement p;
    int *desc;
    int desc_destroy;
    std::string current_class; // "CAS"
    
    std::map<std::string, std::pair<uint32_t, Placement> > *keys_info; // a map from a key to its conf_id and its placement

    int get_timestamp(std::string *key, Timestamp **timestamp, Placement **p);
    int update_placement(std::string &key, uint32_t conf_id = 0); // There must be something on the metadata server with conf_id zero for initialization
    
    Placement* get_placement(std::string &key, bool force_update = false, uint32_t conf_id = 0);

// Logging
#ifdef LOGGING_ON
    
    char log_fmt[64];
    char log_buf[64];
    struct timeval log_tv;
    struct tm *log_tm;

    FILE* log_file;
#endif
};

#endif /* CAS_Client_H */
