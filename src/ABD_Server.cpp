/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   ABD_Server.cpp
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:35 PM
 */

#include "../inc/ABD_Server.h"

using std::string;
using std::vector;
using std::lock_guard;
using std::mutex;

/*
 * KEY = key!ABD_PROTOCOL_NAME!conf_id
 * VALUE = value, ABD_PROTOCOL_NAME, timestamp, reconfig_timestamp, new_conf_id
 * We save KEY->VALUE in the storages
 *
 */

ABD_Server::ABD_Server(const std::shared_ptr<Cache>& cache_p, const std::shared_ptr<Persistent>& persistent_p,
                       const std::shared_ptr<std::mutex>& mu_p) : cache_p(cache_p), persistent_p(persistent_p), mu_p(mu_p){
}

ABD_Server::~ABD_Server(){
}

strVec ABD_Server::get_data(const std::string& key){
    const strVec* ptr = cache_p->get(key);
    if(ptr == nullptr){ // Data is not in cache
        strVec data = persistent_p->get(key);
        return data;
    }
    else{ // data found in cache
        return *ptr;
    }
}

int ABD_Server::put_data(const std::string& key, const strVec& value){
    cache_p->put(key, value);
    persistent_p->put(key, value);
    return S_OK;
}

strVec ABD_Server::init_key(const std::string& key, const uint32_t conf_id){

    DPRINTF(DEBUG_ABD_Server, "started.\n");

    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    strVec init_value{"init", ABD_PROTOCOL_NAME, "0-0", "", ""};
    put_data(con_key, init_value);

    return strVec{"init", ABD_PROTOCOL_NAME, "0-0", "", ""};
}

std::string ABD_Server::get_timestamp(const std::string& key, uint32_t conf_id){

    DPRINTF(DEBUG_ABD_Server, "started.\n");
    lock_guard<mutex> lock(*mu_p);
    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id); // Construct the unique id for the key

    DPRINTF(DEBUG_ABD_Server, "get_timestamp started and the key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_ABD_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        data = init_key(key, conf_id);
    }

    if(data[3] == ""){
        return DataTransfer::serialize({"OK", data[2]});
    }
    else{ // Key reconfigured
        return DataTransfer::serialize({"operation_fail", data[4]});
    }
}

std::string ABD_Server::put(const std::string& key, uint32_t conf_id, const std::string& value, const std::string& timestamp){

    DPRINTF(DEBUG_ABD_Server, "started.\n");
    lock_guard<mutex> lock(*mu_p);

    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id);
    DPRINTF(DEBUG_ABD_Server, "con_key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_ABD_Server, "put new con_key which is %s\n", con_key.c_str());
        strVec val{value, ABD_PROTOCOL_NAME, timestamp, "", ""};
        put_data(con_key, val);
        return DataTransfer::serialize({"OK"});
    }

    if(data[3] == ""){
        if(Timestamp(timestamp) > Timestamp(data[2])){
            data[0] = value;
            data[2] = timestamp;
            put_data(con_key, data);
        }
        return DataTransfer::serialize({"OK"});
    }
    else{ // Key reconfigured
        if(!(Timestamp(timestamp) > Timestamp(data[3]))){
            if(Timestamp(timestamp) > Timestamp(data[2])){
                data[0] = value;
                data[2] = timestamp;
                put_data(con_key, data);
            }
            return DataTransfer::serialize({"OK"});
        }
        else{
            return DataTransfer::serialize({"operation_fail", data[4]});
        }
    }
}

std::string ABD_Server::get(const std::string& key, uint32_t conf_id){

    DPRINTF(DEBUG_ABD_Server, "started.\n");
    lock_guard<mutex> lock(*mu_p);
    string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    DPRINTF(DEBUG_ABD_Server, "get started and the key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_ABD_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        data = init_key(key, conf_id);
    }

    if(data[3] == ""){
        return DataTransfer::serialize({"OK", data[2], data[0]});
    }
    else{ // Key reconfigured
        return DataTransfer::serialize({"operation_fail", data[4]});
    }
}
