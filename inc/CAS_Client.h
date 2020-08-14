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

class CAS_Client {
public:
    CAS_Client(Properties *prop, uint32_t client_id, Placement &pp);
    CAS_Client(Properties *prop, uint32_t client_id, Placement &pp, int desc_l);
    CAS_Client(const CAS_Client& orig) = delete;
    virtual ~CAS_Client();

    uint32_t put(std::string key, std::string value, bool insert = false);
    uint32_t get(std::string key, std::string &value);

private:

    uint32_t id;
    Properties *prop;
    Placement p;
    int desc;
    int desc_destroy;
    std::string current_class; // "CAS"

    uint32_t get_timestamp(std::string *key, Timestamp **timestamp);
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

#endif /* CAS_Client_H */
