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

#include "../inc/Client_Node.h"

Client_Node::Client_Node(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts,
        uint32_t metadata_server_timeout, uint32_t timeout_per_request, std::vector<DC*>& datacenters){

    cas = new CAS_Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout, timeout_per_request,
            datacenters, this);
    abd = new ABD_Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout, timeout_per_request,
            datacenters, this);
}

Client_Node::~Client_Node(){
    if(abd != nullptr){
        delete abd;
        abd = nullptr;
    }
    if(cas != nullptr){
        delete cas;
        cas = nullptr;
    }
}

const uint32_t& Client_Node::get_id() const{
    if(abd != nullptr){
        return abd->get_id();
    }
    if(cas != nullptr){
        return cas->get_id();
    }
    static uint32_t ret = -1;
    return ret;
}

int Client_Node::update_placement(const std::string& key, const uint32_t conf_id){
    
    int ret = 0;
    
    uint32_t requested_conf_id;
    uint32_t new_conf_id; // Not usefull for client
    std::string timestamp; // Not usefull for client
    Placement p;

    if(this->cas != nullptr){
        ret = ask_metadata(this->cas->get_metadata_server_ip(), this->cas->get_metadata_server_port(), key,
                           conf_id, requested_conf_id, new_conf_id, timestamp, p, this->cas->get_retry_attempts(),
                           this->cas->get_metadata_server_timeout());
    }
    else if(this->abd != nullptr){
        ret = ask_metadata(this->abd->get_metadata_server_ip(), this->abd->get_metadata_server_port(), key,
                           conf_id, requested_conf_id, new_conf_id, timestamp, p, this->abd->get_retry_attempts(),
                           this->abd->get_metadata_server_timeout());
    }
    else{
        ret = -1;
        return ret;
    }
    
    assert(ret == 0);
    
    keys_info[key] = std::pair<uint32_t, Placement>(requested_conf_id, p);
//    if(p->protocol == CAS_PROTOCOL_NAME){
//        if(this->desc != -1){
//            destroy_liberasure_instance(((this->desc)));
//        }
//        this->desc = create_liberasure_instance(p);
//        DPRINTF(DEBUG_CAS_Client, "desc is %d\n", desc);
//        fflush(stdout);
//    }
    ret = 0;

    assert(p.m != 0);

//    if(p != nullptr) {
//        delete p;
//        p = nullptr;
//    }
    
    DPRINTF(DEBUG_CAS_Client, "finished\n");
    return ret;
}

const Placement& Client_Node::get_placement(const std::string& key, const bool force_update, const uint32_t conf_id){
    uint64_t le_init = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    
    if(force_update){
        assert(update_placement(key, conf_id) == 0);
        auto it = this->keys_info.find(key);
        DPRINTF(DEBUG_CAS_Client, "latencies: %lu\n", time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
        return it->second.second;
    }
    else{
        auto it = this->keys_info.find(key);
        if(it != this->keys_info.end()){
//            if(it->second.first < conf_id){
            DPRINTF(DEBUG_CAS_Client, "latencies: %lu\n", time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
            return it->second.second;
//            }
//            else{
//                assert(update_placement(key, conf_id) == 0);
//                return this->keys_info[key].second;
//            }
        }
        else{
            assert(update_placement(key, conf_id) == 0);
            DPRINTF(DEBUG_CAS_Client, "latencies: %lu\n", time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
            return this->keys_info[key].second;
        }
    }
    
}

const Placement& Client_Node::get_placement(const std::string& key, const bool force_update, const std::string& conf_id){
    return this->get_placement(key, force_update, stoul(conf_id));
}

const uint32_t& Client_Node::get_conf_id(const std::string& key){
    auto it = this->keys_info.find(key);
    if(it != this->keys_info.end()){
        return it->second.first;
    }
    else{
        assert(update_placement(key, 0) == 0);
        return this->keys_info[key].first;
    }
}

int Client_Node::put(const std::string& key, const std::string& value){
    const Placement& p = get_placement(key);
    if(p.protocol == CAS_PROTOCOL_NAME){
        return this->cas->put(key, value);
    }
    else{
        return this->abd->put(key, value);
    }
}


int Client_Node::get(const std::string& key, std::string& value){
    const Placement& p = get_placement(key);
    if(p.protocol == CAS_PROTOCOL_NAME){
        return this->cas->get(key, value);
    }
    else{
        return this->abd->get(key, value);
    }
}
