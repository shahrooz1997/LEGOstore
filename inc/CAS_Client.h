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
#include "Util.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include "Timestamp.h"

class CAS_Client {
public:
    CAS_Client(Properties &prop, uint32_t local_datacenter_id,
            uint32_t data_center, uint32_t client_id);
    CAS_Client(const CAS_Client& orig) = delete;
    virtual ~CAS_Client();
    
    Timestamp get_timestamp(uint32_t key);
    
    uint32_t put(uint32_t key, std::string value, Placement &p, bool insert = false);
    
    void encode(); // you need to add its inputs
    
private:
    uint32_t id;
    uint32_t data_center;
    uint32_t local_datacenter_id;
    Properties prop; //ToDo: Properties
    
    uint32_t timeout_per_request;
    
    std::string ec_type;
    int32_t timeout;
    std::string current_class; // "Viveck_1"
    std::string encoding_byte; // "latin-1"
    
    
    
    
//    static void _get_timestamp(uint32_t key, std::mutex *mutex,
//                    std::condition_variable *cv, uint32_t *counter, Server *server,
//                    std::string current_class, std::vector<Timestamp*> *tss);
//    
//    static int send_msg(int sock, const std::string &data);

};

#endif /* CAS_Client_H */

