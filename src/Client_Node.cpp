/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Client_Node.cpp
 * Author: shahrooz
 * 
 * Created on August 30, 2020, 5:37 AM
 */

#include "Client_Node.h"

Client_Node::Client_Node(uint32_t client_id, uint32_t local_datacenter_id, std::vector <DC*> &datacenters){
    
    this->desc = -1;
    
    cas = new CAS_Client(client_id, local_datacenter_id, datacenters, &(this->desc), &keys_info);
    abd = nullptr; // Todo: you need to update abd client as well
}

//Client_Node::Client_Node(uint32_t client_id, uint32_t local_datacenter_id, std::vector <DC*> &datacenters, int desc_l){
//    cas = new CAS_Client(client_id, local_datacenter_id, datacenters, desc_l, &keys_info);
//    abd = nullptr; // Todo: you need to update abd client as well
//}


Client_Node::~Client_Node() {
    if(abd != nullptr)
        delete abd;
    if(cas != nullptr)
        delete cas;
}

int Client_Node::update_placement(std::string &key, uint32_t conf_id){
    std::string status, msg;
    Placement* p;
    
//    DPRINTF(DEBUG_CAS_Client, "calling request_placement....\n");
    
    int ret = request_placement(key, conf_id, status, msg, p, DEFAULT_RET_ATTEMPTS, DEFAULT_RET_ATTEMPTS);
    
//    DPRINTF(DEBUG_CAS_Client, "returning request_placement....\n");
    
    assert(ret == 0);
    assert(status == "OK");
    
    
    // Key is not found.
    keys_info[key] = std::pair<uint32_t, Placement>(stoul(msg), *p);
    if(p->protocol == CAS_PROTOCOL_NAME){
        if(this->desc != -1)
            destroy_liberasure_instance(this->desc);
        this->desc = create_liberasure_instance(p);
    }
    
    if(p != nullptr){
        delete p;
        p = nullptr;
    }
    
    DPRINTF(DEBUG_CAS_Client, "finished\n");
    return ret;
}

int Client_Node::put(std::string key, std::string value, bool insert){
    auto it = this->keys_info.find(key);
    if(it == this->keys_info.end()){
        assert(update_placement(key, 0) == 0);
    }
    
    if(this->keys_info[key].second.protocol == CAS_PROTOCOL_NAME){
        return this->cas->put(key, value, insert);
    }
    else{
        return this->abd->put(key, value, insert);
    }
}


int Client_Node::get(std::string key, std::string &value){
    auto it = this->keys_info.find(key);
    if(it == this->keys_info.end()){
        assert(update_placement(key, 0) == 0);
    }
    
    if(this->keys_info[key].second.protocol == CAS_PROTOCOL_NAME){
        return this->cas->get(key, value);
    }
    else{
        return this->abd->get(key, value);
    }
}