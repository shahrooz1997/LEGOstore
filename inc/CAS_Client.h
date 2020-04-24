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
    CAS_Client(Properties &prop, uint32_t client_id);
    CAS_Client(const CAS_Client& orig) = delete;
    virtual ~CAS_Client();

    uint32_t put(std::string key, std::string value, Placement &p, bool insert = false);
    uint32_t get(std::string key, std::string &value, Placement &p);

private:
    uint32_t id;
    Properties prop;
    std::string current_class; // "CAS"

    Timestamp* get_timestamp(std::string *key, Placement &p);
};

#endif /* CAS_Client_H */
