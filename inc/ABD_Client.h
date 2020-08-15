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
    ABD_Client(Properties *prop, uint32_t client_id, Placement &pp);
//    ABD_Client(Properties &prop, uint32_t client_id);
    ABD_Client(const ABD_Client& orig) = delete;
    virtual ~ABD_Client();
    
    uint32_t put(std::string key, std::string value, bool insert = false);
    uint32_t get(std::string key, std::string &value);
    
//    uint32_t get_operation_id();
    
private:
    
    uint32_t id;
    Properties *prop;
    Placement p;
    std::string current_class; // "ABD"
    
//    uint32_t operation_id;

    uint32_t get_timestamp(std::string *key, Timestamp **timestamp, std::string &value);
    void update_placement(std::string &new_cfg);

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
