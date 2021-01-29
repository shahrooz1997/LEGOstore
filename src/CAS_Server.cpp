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

CAS_Server::CAS_Server(const std::shared_ptr<Cache>& cache_p, const std::shared_ptr<Persistent>& persistent_p,
                       const std::shared_ptr<std::mutex>& mu_p) : cache_p(cache_p), persistent_p(persistent_p), mu_p(mu_p){
}

CAS_Server::~CAS_Server(){
}

strVec CAS_Server::init_key(const string& key, const uint32_t conf_id){

    DPRINTF(DEBUG_CAS_Server, "started.\n");

    std::string con_key2 = construct_key(key, CAS_PROTOCOL_NAME, conf_id, "0-0");
    std::vector<std::string> init_value2{"CAS_Server::get_timestamp_val", "init", CAS_PROTOCOL_NAME, "0-0"};
    persistent_p->merge(con_key2, init_value2);

    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    std::vector<std::string> init_value{"CAS_Server::get_timestamp_ts", "0-0", "", ""};
    persistent_p->merge(con_key, init_value);

    return strVec{"0-0", "", ""};
}

bool CAS_Server::is_op_failed(const std::string& key, const uint32_t conf_id, const string& timestamp, string& recon_ts){
    DPRINTF(DEBUG_CAS_Server, "started.\n");

    string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
    recon_ts.clear();

    strVec data = persistent_p->get(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_CAS_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        data = init_key(key, conf_id);
        assert(!data.empty());
    }

    if(data[1] != ""){ // Key reconfigured
        if(Timestamp(timestamp) > Timestamp(data[1])){
            recon_ts = data[2];
            return true;
        }
    }
    return false;
}

std::string CAS_Server::get_timestamp(const std::string& key, uint32_t conf_id){

    DPRINTF(DEBUG_CAS_Server, "started.\n");
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id); // Construct the unique id for the key

    DPRINTF(DEBUG_CAS_Server, "get_timestamp started and the key is %s\n", con_key.c_str());

    strVec data = persistent_p->get(con_key);
    if(data.empty()){
        DPRINTF(DEBUG_CAS_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(), conf_id);
        data = init_key(key, conf_id);
        assert(!data.empty());
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
//    lock_guard<mutex> lock(*mu_p);

    string recon_ts;
    if(is_op_failed(key, conf_id, timestamp, recon_ts)){
        return DataTransfer::serialize({"operation_fail", recon_ts});
    }

    string con_key2 = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    DPRINTF(DEBUG_CAS_Server, "con_key2 is %s\n", con_key2.c_str());

    vector<string> val2{"CAS_Server::put", value, CAS_PROTOCOL_NAME, timestamp};
    persistent_p->merge(con_key2, val2);

    return DataTransfer::serialize({"OK"});
}

std::string CAS_Server::put_fin(const std::string& key, uint32_t conf_id, const std::string& timestamp){

    DPRINTF(DEBUG_CAS_Server, "started.\n");

    string recon_ts;
    if(is_op_failed(key, conf_id, timestamp, recon_ts)){
        return DataTransfer::serialize({"operation_fail", recon_ts});
    }

    string con_key2 = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    DPRINTF(DEBUG_CAS_Server, "con_key2 is %s\n", con_key2.c_str());

    vector<string> val2{"CAS_Server::put_fin_val", "", CAS_PROTOCOL_NAME, timestamp};
    persistent_p->merge(con_key2, val2);

    string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
    vector<string> val{"CAS_Server::put_fin_ts", timestamp};
    persistent_p->merge(con_key, val);

    return DataTransfer::serialize({"OK"});
}

std::string CAS_Server::get(const std::string& key, uint32_t conf_id, const std::string& timestamp){

    DPRINTF(DEBUG_CAS_Server, "started.\n");

    string recon_ts;
    if(is_op_failed(key, conf_id, timestamp, recon_ts)){
        return DataTransfer::serialize({"operation_fail", recon_ts});
    }

    string con_key2 = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    DPRINTF(DEBUG_CAS_Server, "con_key2 is %s\n", con_key2.c_str());

    vector<string> val2{"CAS_Server::get_val", "", CAS_PROTOCOL_NAME, timestamp};
    persistent_p->merge(con_key2, val2);

    string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
    vector<string> val{"CAS_Server::get_ts", timestamp};
    persistent_p->merge(con_key, val);

    strVec data = persistent_p->get(con_key2);
    if(data[0] != ""){
        return DataTransfer::serialize({"OK", data[0]});
    }
    else{
        return DataTransfer::serialize({"OK", "Ack"});
    }
}
