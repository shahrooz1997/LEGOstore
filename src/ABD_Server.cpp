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

#include "ABD_Server.h"

using std::string;

// Data Storage Format
// key -> (value, timestamp)

Reconfig_key_info* ABD_Server::create_rki(const std::string& key, const uint32_t conf_id){
    Reconfig_key_info* ret = new Reconfig_key_info;
    ret->curr_conf_id = conf_id;
    ret->curr_placement = nullptr;
    ret->reconfig_state = 0;
    ret->timestamp = ""; // Todo: maybe you need to add it into the metadata server
    ret->next_conf_id = -1;
    ret->next_placement = nullptr;
    
    //Todo: shahrooz: get placement info from metadata server
    Connect c(*(this->metadata_server_ip), *(this->metadata_server_port));
    if(!c.is_connected()){
        DPRINTF(DEBUG_ABD_Server, "Metadata Server is down.\n");
        delete ret;
        ret = nullptr;
        return ret;
    }
    DataTransfer::sendMsg(*c, DataTransfer::serializeMDS("ask", key + "!" + std::to_string(conf_id)));
    std::string recvd;
    if(DataTransfer::recvMsg(*c, recvd) == 1){
        std::string status;
        std::string msg;
        ret->curr_placement = DataTransfer::deserializeMDS(recvd, status, msg);
        if(status != "OK"){
//            if(stoul(msg) > conf_id){
//                rki.reconfig_state = 2;
//                rki.next_conf_id = stoul(msg);
//                // Todo: we may need to fill next_placement and timestamp fields too
//            }
            delete ret;
            ret = nullptr;
            DPRINTF(DEBUG_ABD_Server, "Metadata Server ERROR: msg is %s.\n", msg.c_str());
        }
        
    }
    else{
        delete ret;
        ret = nullptr;
    }
    
    return ret;
}

ABD_Server::ABD_Server(std::map <std::string, std::vector<Request>>* recon_keys, std::string* metadata_server_ip,
        std::string* metadata_server_port) : recon_keys(recon_keys), metadata_server_ip(metadata_server_ip),
        metadata_server_port(metadata_server_port){
}

ABD_Server::~ABD_Server(){
}

int ABD_Server::init_key(const string& key, const uint32_t conf_id, Cache& cache, Persistent& persistent){
    
    DPRINTF(DEBUG_ABD_Server, "started.\n");
    
    int ret = 0;
    
    std::string timestamp = "0-0";
    std::string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id);
    
    Reconfig_key_info* rki = create_rki(key, conf_id);
    if(rki == nullptr){
        return -1;
    }
    
    std::vector <std::string> value{"init", ABD_PROTOCOL_NAME, timestamp, rki->get_string(), "FIN"};
    
    if(rki != nullptr){
        delete rki;
    }
    
    cache.put(con_key, value);
    persistent.put(con_key, value);
    
    return ret;
}

int
ABD_Server::reconfig_info(const string& key, uint32_t conf_id, const Request& req, string& msg, string& recon_timestamp,
        Cache& cache, Persistent& persistent){
    DPRINTF(DEBUG_ABD_Server, "started.\n");
    
    int ret = 0;
    std::string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id);
    
    strVec data;
    
    bool fnd = cache.exists(con_key);
    if(!fnd){
        fnd = persistent.exists(con_key);
        if(!fnd){
            return -1; // You should never get this error code!
        }
        
        data = persistent.get(con_key);
    }
    else{
        data = (*cache.get(con_key));
    }

//    DPRINTF(DEBUG_ABD_Server, "creating Reconfig_key_info\n");
//    return 0;
    Reconfig_key_info rki(data[3]);

//    DPRINTF(DEBUG_ABD_Server, "Reconfig_key_info created\n");
    
    if(conf_id != rki.curr_conf_id){
        DPRINTF(DEBUG_ABD_Server, "BAD Request from client to the wrong server\n");
        ret = -2;
    }
    else{
        if(rki.reconfig_state == 0){
            ret = 0;
        }
        else if(rki.reconfig_state == 1){ // block
            //        recon_keys[key].push_back(req);
            recon_timestamp = rki.timestamp; // Todo: it is not set yet
            ret = 1;
        }
        else if(rki.reconfig_state == 2){ // Reconfigured
            recon_timestamp = rki.timestamp;
            msg = std::to_string(rki.next_conf_id);
            ret = 2;
        }
        else{
            DPRINTF(DEBUG_ABD_Server, "Bad value in reconfig_state = %d\n", rki.reconfig_state);
            ret = -3; // ERROR: Bad value in reconfig_state
        }
    }

//    DPRINTF(DEBUG_ABD_Server,"recon_status is %d\n", ret);
    
    DPRINTF(DEBUG_ABD_Server, "finished with return value %d.\n", ret);
    return ret;
}

std::string
ABD_Server::get_timestamp(string& key, uint32_t conf_id, const Request& req, Cache& cache, Persistent& persistent,
        std::mutex& lock_t){
    
    std::lock_guard <std::mutex> lock(lock_t);
    DPRINTF(DEBUG_ABD_Server, "started.\n");
    std::string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    
    DPRINTF(DEBUG_ABD_Server, "get_timestamp started and the key is %s\n", con_key.c_str());
    const std::vector <std::string>* ptr = cache.get(con_key);
    strVec data; // = (*cache.get(key))[0];
    strVec result;
    
    if(ptr == nullptr){
        data = persistent.get(con_key);
        DPRINTF(DEBUG_ABD_Server, "cache data checked and data is null\n");
        if(data.empty()){
//            result = {"Failed", "None"}; // This is not a failure. We should init the key and sent the least possible timestamp for the key
            // We should initialize the uninitialized key.
            DPRINTF(DEBUG_ABD_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(),
                    conf_id);
            init_key(key, conf_id, cache, persistent);
            result = {"OK", "0-0"};
            return DataTransfer::serialize(result);
        }
        else{
            DPRINTF(DEBUG_ABD_Server, "Found data in persisten. sending back! data is"\
                    "key:%s   timestamp:%s\n", con_key.c_str(), data[0].c_str());
        }
    }
    else{
        data = (*ptr);
        DPRINTF(DEBUG_ABD_Server, "cache data checked and data is %s\n", data[0].c_str());
    }
    
    // Data is found
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, req, msg, recon_timestamp, cache, persistent);
    
    if(recon_status == 0){
        result = {"OK", data[2]};
        return DataTransfer::serialize(result);
    }
    else if(recon_status == 1){ // Todo: implement a mechanism to take care of blocked operations
        (*recon_keys)[key].push_back(req);
        return "SEND_NOTHING";
        // Todo: Do nothing, it will be taken care of later
    }
    else if(recon_status == 2){
        result = {"operation_fail", msg}; // msg is the confid of the new placement
        return DataTransfer::serialize(result);
    }
    else{
        DPRINTF(DEBUG_ABD_Server, "something bad happened\n");
        assert(false);
        result = {"ERROR", "INTERNAL ERROR"};
        return DataTransfer::serialize(result);
    }
}


std::string
ABD_Server::put(string& key, uint32_t conf_id, string& value, string& timestamp, const Request& req, Cache& cache,
        Persistent& persistent, std::mutex& lock_t){
    
    
    std::lock_guard <std::mutex> lock(lock_t);
    DPRINTF(DEBUG_ABD_Server, "started.\n");
    
    std::string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id);
    DPRINTF(DEBUG_ABD_Server, "con_key is %s\n", con_key.c_str());
    
    bool fnd = cache.exists(con_key);
    bool flag = false;
    strVec data;
    
    if(!fnd){
        fnd = persistent.exists(con_key);
        
        // If Tag not seen before, add the tuple
        if(!fnd){
            if(!value.empty()){
                
                Reconfig_key_info* rki = create_rki(key, conf_id);
                
                //Todo: shahrooz: add previous timestamp to the end of the value
                std::vector <std::string> val{value, ABD_PROTOCOL_NAME, timestamp, rki->get_string(), "FIN"};
                
                DPRINTF(DEBUG_ABD_Server, "put new con_key which is %s\n", con_key.c_str());
                cache.put(con_key, val);
                persistent.put(con_key, val);
                flag = true;
                return DataTransfer::serialize({"OK"}); // Todo: make sure that it is correct to comment out this line
            }
            else{
                assert(false);
            }
        }
        else{
            data = persistent.get(con_key);
            DPRINTF(DEBUG_ABD_Server, "WARN: key %s found in persistent\n", con_key.c_str());
            //TODO:: Need to insert in cache where. otherwise modify being done in next steps maybe invalid
        }
    }
    else{
        data = *(cache.get(con_key)); // = (*cache.get(key))[0];
        DPRINTF(DEBUG_ABD_Server, "WARN: key %s found in cache\n", con_key.c_str());
    }
    
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, req, msg, recon_timestamp, cache, persistent);
    DPRINTF(DEBUG_ABD_Server, "recon_status is %d\n", recon_status);
    if(recon_status == 0){
        if(Timestamp(timestamp) > Timestamp(data[2])){
            data[0] = value;
            data[2] = timestamp;
            
            cache.put(con_key, data);
            persistent.put(con_key, data);
        }
        
        return DataTransfer::serialize({"OK"});
    }
    else if(recon_status == 1 && !flag){
        (*recon_keys)[key].push_back(req);
        return DataTransfer::serialize({"SEND_NOTHING"});;
        // Do nothing, it will be taken care of later
    }
    else if(recon_status == 2){
        if(recon_timestamp != ""){
            Timestamp temp(recon_timestamp);
            Timestamp c_temp(timestamp);
            if(!(temp > c_temp)){
                return DataTransfer::serialize({"OK"});
            }
            else{
                return DataTransfer::serialize({"operation_fail", msg});
            }
        }
        else{
            return DataTransfer::serialize({"operation_fail", msg});
        }
    }
    else{
        DPRINTF(DEBUG_ABD_Server, "something bad happened\n");
        assert(false);
        return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
    }
}


std::string ABD_Server::get(string& key, uint32_t conf_id, string& timestamp, const Request& req, Cache& cache,
        Persistent& persistent, std::mutex& lock_t){
    
    std::lock_guard <std::mutex> lock(lock_t);
    DPRINTF(DEBUG_ABD_Server, "started.\n");
    std::string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    
    DPRINTF(DEBUG_ABD_Server, "get started and the key is %s\n", con_key.c_str());
    const std::vector <std::string>* ptr = cache.get(con_key);
    strVec data; // = (*cache.get(key))[0];
    strVec result;
    
    if(ptr == nullptr){
        data = persistent.get(con_key);
        DPRINTF(DEBUG_ABD_Server, "cache data checked and data is null\n");
        if(data.empty()){
//            result = {"Failed", "None"}; // This is not a failure. We should init the key and sent the least possible timestamp for the key
            // We should initialize the uninitialized key.
            DPRINTF(DEBUG_ABD_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(),
                    conf_id);
            init_key(key, conf_id, cache, persistent);
            result = {"OK", "0-0", "init"};
            return DataTransfer::serialize(result);
        }
        else{
            DPRINTF(DEBUG_ABD_Server, "Found data in persisten. sending back! data is"\
                    "key:%s   timestamp:%s\n", con_key.c_str(), data[0].c_str());
        }
    }
    else{
        data = (*ptr);
        DPRINTF(DEBUG_ABD_Server, "cache data checked and data is %s\n", data[0].c_str());
    }
    
    // Data is found
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, req, msg, recon_timestamp, cache, persistent);
    
    if(recon_status == 0){
        result = {"OK", data[2], data[0]}; // timestamp, value
        return DataTransfer::serialize(result);
    }
    else if(recon_status == 1){ // Todo: implement a mechanism to take care of blocked operations
        (*recon_keys)[key].push_back(req);
        return "SEND_NOTHING";
        // Todo: Do nothing, it will be taken care of later
    }
    else if(recon_status == 2){
        result = {"operation_fail", msg}; // msg is the confid of the new placement
        return DataTransfer::serialize(result);
    }
    else{
        DPRINTF(DEBUG_ABD_Server, "something bad happened\n");
        assert(false);
        result = {"ERROR", "INTERNAL ERROR"};
        return DataTransfer::serialize(result);
    }
}
