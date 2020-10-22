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

#include "Client.h"
#include <erasurecode.h>

class Client_Node;

class CAS_Client : public Client{
public:
    CAS_Client(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
            uint32_t timeout_per_request, std::vector<DC*>& datacenters, int* desc, Client_Node* parent);
    
    CAS_Client(const CAS_Client& orig) = delete;
    
    virtual ~CAS_Client();
    
    int put(const std::string& key, const std::string& value);
    
    int get(const std::string& key, std::string& value);


private:
    int* desc;
    Client_Node* parent;

    bool can_be_optimized;
    
    int get_timestamp(const std::string& key, Timestamp*& timestamp);
};

#endif /* CAS_Client_H */
