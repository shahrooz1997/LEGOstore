/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   CAS_Server.cpp
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:35 PM
 */

#include "../inc/CAS_Server.h"

using std::string;
using std::vector;
using std::lock_guard;
using std::mutex;

/*
 * KEY1 = key!CAS_PROTOCOL_NAME!conf_id
 * VALUE1 = latest_timestamp, reconfig_timestamp, new_conf_id
 *
 * KEY2 = key!CAS_PROTOCOL_NAME!conf_id!timestamp
 * VALUE2 = value, CAS_PROTOCOL_NAME, timestamp, "PRE"/"FIN"
 * We save KEY->VALUE in the storages
 *
 */

strVec CAS_Server::get_data(const std::string& key){
    const strVec* ptr = cache_p->get(key);
    if(ptr == nullptr){ // Data is not in cache
        strVec data = persistent_p->get(key);
        return data;
    }
    else{ // data found in cache
        return *ptr;
    }
}

int CAS_Server::put_data(const std::string& key, const strVec& value){
    cache_p->put(key, value);
    persistent_p->put(key, value);
    return S_OK;
}

CAS_Server::CAS_Server(const std::shared_ptr<Cache>& cache_p, const std::shared_ptr<Persistent>& persistent_p,
                       const std::shared_ptr<std::mutex>& mu_p) : cache_p(cache_p), persistent_p(persistent_p), mu_p(mu_p){
}

CAS_Server::~CAS_Server(){
}

int CAS_Server::init_key(const string& key, const uint32_t conf_id){
    
    DPRINTF(DEBUG_CAS_Server, "started.\n");
    
    int ret = 0;
    std::string timestamp = "0-0";
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
    std::string con_key2 = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);

    std::vector <std::string> value{timestamp, "", ""};
    std::vector <std::string> value2{"init", CAS_PROTOCOL_NAME, timestamp, "FIN"};

    put_data(con_key, value);
    put_data(con_key2, value2);

    return ret;
}

std::string CAS_Server::get_timestamp(const std::string& key, uint32_t conf_id){

    DPRINTF(DEBUG_CAS_Server, "started.\n");
    lock_guard<mutex> lock(*mu_p);
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    
    DPRINTF(DEBUG_CAS_Server, "get_timestamp started and the key is %s\n", con_key.c_str());

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_CAS_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        init_key(key, conf_id);
        return DataTransfer::serialize({"OK", "0-0"});
    }

    if(data[1] == ""){
        return DataTransfer::serialize({"OK", data[0]});
    }
    else{ // Key reconfigured
        return DataTransfer::serialize({"operation_fail", data[2]});
    }
}

std::string CAS_Server::put(const std::string& key, uint32_t conf_id, const std::string& value, const std::string& timestamp){

    DPRINTF(DEBUG_CAS_Server, "started.\n");
    lock_guard<mutex> lock(*mu_p);
    string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_CAS_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        init_key(key, conf_id);
        data = get_data(con_key);
    }

    if(data[1] != ""){ // Key reconfigured
        if(Timestamp(timestamp) > Timestamp(data[1])){
            return DataTransfer::serialize({"operation_fail", data[2]});
        }
    }

    string con_key2 = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    DPRINTF(DEBUG_CAS_Server, "con_key2 is %s\n", con_key2.c_str());

    data = get_data(con_key2);
    if(data.empty()){
        DPRINTF(DEBUG_CAS_Server, "put new con_key which is %s\n", con_key.c_str());
        std::vector <std::string> val2{value, CAS_PROTOCOL_NAME, timestamp, "PRE"};
        put_data(con_key2, val2);
        return DataTransfer::serialize({"OK"});
    }

    // Ignore repetitive messages
    return DataTransfer::serialize({"OK"});
}

std::string CAS_Server::put_fin(const std::string& key, uint32_t conf_id, const std::string& timestamp){

    DPRINTF(DEBUG_CAS_Server, "started.\n");
    lock_guard<mutex> lock(*mu_p);
    string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_CAS_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        init_key(key, conf_id);
        data = get_data(con_key);
    }

    if(data[1] != ""){ // Key reconfigured
        if(Timestamp(timestamp) > Timestamp(data[1])){
            return DataTransfer::serialize({"operation_fail", data[2]});
        }
    }

    if(Timestamp(timestamp) > Timestamp(data[0])){
        data[0] = timestamp;
        put_data(con_key, data);
    }

    string con_key2 = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    DPRINTF(DEBUG_CAS_Server, "con_key2 is %s\n", con_key2.c_str());

    data = get_data(con_key2);
    if(data.empty()){
        DPRINTF(DEBUG_CAS_Server, "put new con_key which is %s\n", con_key.c_str());
        std::vector <std::string> val2{"", CAS_PROTOCOL_NAME, timestamp, "FIN"};
        put_data(con_key2, val2);
        return DataTransfer::serialize({"OK"});
    }

    data[3] = "FIN";
    put_data(con_key2, data);
    return DataTransfer::serialize({"OK"});
}

std::string CAS_Server::get(const std::string& key, uint32_t conf_id, const std::string& timestamp){

    DPRINTF(DEBUG_CAS_Server, "started.\n");
    lock_guard<mutex> lock(*mu_p);
    string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);

    strVec data = get_data(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_CAS_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        init_key(key, conf_id);
        data = get_data(con_key);
    }

    if(data[1] != ""){ // Key reconfigured
        if(Timestamp(timestamp) > Timestamp(data[1])){
            return DataTransfer::serialize({"operation_fail", data[2]});
        }
    }

    if(Timestamp(timestamp) > Timestamp(data[0])){
        data[0] = timestamp;
        put_data(con_key, data);
    }

    std::string con_key2 = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    DPRINTF(DEBUG_CAS_Server, "con_key2 is %s\n", con_key2.c_str());

    data = get_data(con_key2);
    if(data.empty()){
        DPRINTF(DEBUG_CAS_Server, "put new con_key which is %s\n", con_key.c_str());
        std::vector <std::string> val2{"", CAS_PROTOCOL_NAME, timestamp, "FIN"};
        put_data(con_key2, val2);
        return DataTransfer::serialize({"OK", "Ack"});
    }

    data[3] = "FIN";
    put_data(con_key2, data);
    if(data[0] != ""){
        return DataTransfer::serialize({"OK", data[0]});
    }
    else{
        return DataTransfer::serialize({"OK", "Ack"});
    }
}
