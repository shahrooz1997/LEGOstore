/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   ABDClient.h
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:34 PM
 */

#ifndef ABD_Client_H
#define ABD_Client_H

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
#include "Data_Transfer.h"

class ABD_Client {
public:
    ABD_Client(uint32_t client_id, uint32_t local_datacenter_id, std::vector <DC*> &datacenters, int *desc_l, std::map<std::string, std::pair<uint32_t, Placement> > *keys_info);
//    ABD_Client(Properties &prop, uint32_t client_id);
    ABD_Client(const ABD_Client& orig) = delete;
    virtual ~ABD_Client();
    
    int put(std::string key, std::string value, bool insert = false);
    int get(std::string key, std::string &value);
    
//    uint32_t get_operation_id();
    
private:
    
    uint32_t                local_datacenter_id;
    uint32_t                retry_attempts;
    uint32_t                metadata_server_timeout;
    uint32_t                timeout_per_request;
    uint64_t                start_time;
    std::vector <DC*>       datacenters; // Should not be deleted in Client destructor
    
    uint32_t id;
    std::string current_class; // "ABD"
    
     int *desc;
    
//    uint32_t operation_id;
   
    std::map<std::string, std::pair<uint32_t, Placement> > *keys_info; // a map from a key to its conf_id and its placement

    int get_timestamp(std::string *key, Timestamp **timestamp, Placement **p);
    int update_placement(const std::string &key, const uint32_t conf_id = 0); // There must be something on the metadata server with conf_id zero for initialization
    
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

#endif /* ABDCLIENT_H */
