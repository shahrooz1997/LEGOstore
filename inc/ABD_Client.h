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

#include "Client.h"

class Client_Node;

class ABD_Client : public Client{
public:
    ABD_Client(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
            uint32_t timeout_per_request, std::vector<DC*>& datacenters, Client_Node* parent);
    
    ABD_Client(const ABD_Client& orig) = delete;
    
    virtual ~ABD_Client();
    
    int put(const std::string& key, const std::string& value);
    
    int get(const std::string& key, std::string& value);

private:
    Client_Node* parent;
    
    int get_timestamp(const std::string& key, Timestamp*& timestamp);

};

#endif /* ABDCLIENT_H */
