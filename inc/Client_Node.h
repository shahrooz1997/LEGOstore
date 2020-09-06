/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Client_Node.h
 * Author: shahrooz
 *
 * Created on August 30, 2020, 5:37 AM
 */

#ifndef CLIENT_NODE_H
#define CLIENT_NODE_H

#include "CAS_Client.h"
#include "ABD_Client.h"

class Client_Node {
public:
//    Client_Node();
    Client_Node(uint32_t client_id, uint32_t local_datacenter_id, std::vector <DC*> &datacenters);
//    Client_Node(uint32_t client_id, uint32_t local_datacenter_id, std::vector <DC*> &datacenters, int desc_l);
    Client_Node(const Client_Node& orig) = delete;
    virtual ~Client_Node();
    
    int put(std::string key, std::string value, bool insert = false);
    int get(std::string key, std::string &value);
    
    
    // getters
    uint32_t get_id();

private:
    
    int update_placement(std::string &key, uint32_t conf_id = 0);
    std::map<std::string, std::pair<uint32_t, Placement> > keys_info; // a map from a key to its conf_id and its placement
    
    ABD_Client *abd;
    CAS_Client *cas;
    
    int desc;
    
};

#endif /* CLIENT_NODE_H */

