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



// NOTE:: Shahrooz: in the future, we will change the structure to have a link list of timestamp for each key. add pv_timestamp to Data_handler struct and you know the continuation.
//        Shahrooz: Maybe, it is better to have a list of timestamps for each key.


// Todo: update the server code so when they are notified by the metadata server that a reconfiguration has been occurred send back the correct message to the clients


#include "CAS_Server.h"

using std::string;

Reconfig_key_info* CAS_Server::create_rki(const std::string& key, const uint32_t conf_id){
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
        DPRINTF(DEBUG_CAS_Server, "Metadata Server is down.\n");
        delete ret;
        ret = nullptr;
        return ret;
    }
    DataTransfer::sendMsg(*c, DataTransfer::serializeMDS("ask", key + "!" + std::to_string(conf_id)));
    std::string recvd;
    if(DataTransfer::recvMsg(*c, recvd) == 1){
//        DPRINTF(DEBUG_CAS_Server, "loed test rki\n");
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
            DPRINTF(DEBUG_CAS_Server, "Metadata Server ERROR: msg is %s.\n", msg.c_str());
        }
        
    }
    else{
//        DPRINTF(DEBUG_CAS_Server, "loed Error in receiving msg from Metadata Server\n");
        delete ret;
        ret = nullptr;
    }
    
    return ret;
}


CAS_Server::CAS_Server(std::map <std::string, std::vector<Request>>* recon_keys, std::string* metadata_server_ip,
        std::string* metadata_server_port) : recon_keys(recon_keys), metadata_server_ip(metadata_server_ip),
        metadata_server_port(metadata_server_port){
    this->recon_keys = recon_keys;
}


CAS_Server::~CAS_Server(){
}

// Returns true if success, i.e., timestamp added as the fin timestamp for the key
static bool complete_fin(const std::string& key, const uint32_t conf_id, const std::string& timestamp, Cache& cache,
        Persistent& persistent){
    DPRINTF(DEBUG_CAS_Server, "started.\n");
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
    
    strVec data;
    bool fnd = cache.exists(con_key);
    if(!fnd){
        fnd = persistent.exists(con_key);
        if(!fnd){
            cache.put(con_key, {timestamp, "nb"}); // nb: not-blocked, b: blocked
            persistent.put(con_key, {timestamp, "nb"}); // nb: not-blocked, b: blocked
            return true;
        }
        data = persistent.get(con_key);
    }
    else{
        data = *cache.get(con_key);
    }
    
    std::string saved_timestamp = data[0];
    
    if(Timestamp(timestamp) > Timestamp(saved_timestamp)){
        cache.put(con_key, {timestamp, "nb"}); // nb: not-blocked, b: blocked
        persistent.put(con_key, {timestamp, "nb"}); // nb: not-blocked, b: blocked
    }
    
    DPRINTF(DEBUG_CAS_Server, "finished.\n");
    
    return true;
}

int CAS_Server::reconfig_info(const string& key, uint32_t conf_id, string& timestamp, const Request& req, string& msg,
        string& recon_timestamp, Cache& cache, Persistent& persistent){
    DPRINTF(DEBUG_CAS_Server, "started.\n");
    
    int ret = 0;
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    
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

//    DPRINTF(DEBUG_CAS_Server, "creating Reconfig_key_info\n");
//    return 0;
    Reconfig_key_info rki(data[3]);

//    DPRINTF(DEBUG_CAS_Server, "Reconfig_key_info created\n");
    
    if(conf_id != rki.curr_conf_id){
        DPRINTF(DEBUG_CAS_Server, "BAD Request from client to the wrong server\n");
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
            DPRINTF(DEBUG_CAS_Server, "Bad value in reconfig_state = %d\n", rki.reconfig_state);
            ret = -3; // ERROR: Bad value in reconfig_state
        }
    }

//    DPRINTF(DEBUG_CAS_Server,"recon_status is %d\n", ret);
    
    DPRINTF(DEBUG_CAS_Server, "finished with return value %d.\n", ret);
    return ret;
}

int CAS_Server::init_key(const string& key, const uint32_t conf_id, Cache& cache, Persistent& persistent){
    
    DPRINTF(DEBUG_CAS_Server, "started.\n");
    
    int ret = 0;
    
    std::string timestamp = "0-0";
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
    std::string con_key2 = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    
    Reconfig_key_info* rki = create_rki(key, conf_id);
    if(rki == nullptr){
        return -1;
    }
    
    std::vector <std::string> value{"init", CAS_PROTOCOL_NAME, timestamp, rki->get_string(), "FIN"};
    
    if(rki != nullptr){
        delete rki;
    }
    
    cache.put(con_key2, value);
    persistent.put(con_key2, value);
    complete_fin(key, conf_id, timestamp, cache, persistent);
    
    DPRINTF(DEBUG_CAS_Server, "finished with return value %d.\n", ret);
    return ret;
}

std::string
CAS_Server::get_timestamp(const string& key, uint32_t conf_id, const Request& req, Cache& cache, Persistent& persistent,
        std::mutex& lock_t){
    
    std::lock_guard <std::mutex> lock(lock_t);
    DPRINTF(DEBUG_CAS_Server, "started.\n");
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id); // Construct the unique id for the key
    
    DPRINTF(DEBUG_CAS_Server, "get_timestamp started and the key is %s\n", con_key.c_str());
    const std::vector <std::string>* ptr = cache.get(con_key);
    strVec data; // = (*cache.get(key))[0];
    strVec result;
    
    if(ptr == nullptr){
        data = persistent.get(con_key);
        DPRINTF(DEBUG_CAS_Server, "cache data checked and data is null\n");
        if(data.empty()){
//            result = {"Failed", "None"}; // This is not a failure. We should init the key and sent the least possible timestamp for the key
            // We should initialize the uninitialized key.
            DPRINTF(DEBUG_CAS_Server, "WARN: Key %s with confid %d was not found! Initializing...\n", key.c_str(),
                    conf_id);
            if(init_key(key, conf_id, cache, persistent) != 0){
                result = {"ERROR", "INTERNAL ERROR"};
            }
            else{
                result = {"OK", "0-0"};
            }
            DPRINTF(DEBUG_CAS_Server, "finished.\n");
            return DataTransfer::serialize(result);
        }
        else{
            DPRINTF(DEBUG_CAS_Server, "Found data in persisten. sending back! data is"\
                    "key:%s   timestamp:%s\n", con_key.c_str(), data[0].c_str());
        }
    }
    else{
        data = (*ptr);
        DPRINTF(DEBUG_CAS_Server, "cache data checked and data is %s\n", data[0].c_str());
    }
    
    // Data is found
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, data[0], req, msg, recon_timestamp, cache, persistent);
    
    if(recon_status == 0){
        result = {"OK", data[0]};
        DPRINTF(DEBUG_CAS_Server, "finished.\n");
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
        DPRINTF(DEBUG_CAS_Server, "something bad happened\n");
        assert(false);
        result = {"ERROR", "INTERNAL ERROR"};
        return DataTransfer::serialize(result);
    }
}

std::string
CAS_Server::put(string& key, uint32_t conf_id, string& val, string& timestamp, const Request& req, Cache& cache,
        Persistent& persistent, std::mutex& lock_t){
    
    std::lock_guard <std::mutex> lock(lock_t);
    DPRINTF(DEBUG_CAS_Server, "started.\n");
//	DPRINTF(DEBUG_CAS_Server,"put function timestamp %s\n", timestamp.c_str());
//	insert_data(key, conf_id, value, timestamp, false, cache, persistent);
//	return DataTransfer::serialize({"OK"});
    
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    DPRINTF(DEBUG_CAS_Server, "con_key is %s\n", con_key.c_str());

//    int _label = label? 1:0;
    
    //TODO:: ALready in FIN, so no way that we need to store naything , cos no way values
    // can come now in a msg right???
    //TODO:: should I add it to cache on write, from persistent
    //If not found, that means either tuple not present
    // OR tuple is in pre stage
    // chekcing to set FIN flag
    bool fnd = cache.exists(con_key);
    
    bool flag = false;
    
    if(!fnd){
        fnd = persistent.exists(con_key);
        
        // If Tag not seen before, add the tuple
        if(!fnd){
            if(!val.empty()){

                Reconfig_key_info* rki = create_rki(key, conf_id);
                if(rki == nullptr){
                    return {"ERROR", "INTERNAL ERROR"};
                }
                
                //Todo: shahrooz: add previous timestamp to the end of the value
                std::vector <std::string> value{val, CAS_PROTOCOL_NAME, timestamp, rki->get_string(), "PRE"};
                
                if(rki != nullptr){
                    delete rki;
                }
                
                DPRINTF(DEBUG_CAS_Server, "put new con_key which is %s\n", con_key.c_str());
                ///TODO:: optimize and create threads here
                // For thread, add label check here and then return
//                DPRINTF(DEBUG_CAS_Server,"Adding entries to both cache and persitent\n");
                // Also add fin tag to both
                cache.put(con_key, value);
                persistent.put(con_key, value);
                flag = true;
                DPRINTF(DEBUG_CAS_Server, "finished.\n");
                return DataTransfer::serialize({"OK"}); // Todo: make sure that it is correct to comment out this line
            }
            else{
                assert(false);
            }
        }
        else{
            DPRINTF(DEBUG_CAS_Server, "WARN: key %s found in persistent\n", con_key.c_str());
            //TODO:: Need to insert in cache where. otherwise modify being done in next steps maybe invalid
        }
    }
    else{
        DPRINTF(DEBUG_CAS_Server, "WARN: key %s found in cache\n", con_key.c_str());
    }
    
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, timestamp, req, msg, recon_timestamp, cache, persistent);
    DPRINTF(DEBUG_CAS_Server, "recon_status is %d\n", recon_status);
    if(recon_status == 0){
        DPRINTF(DEBUG_CAS_Server, "finished.\n");
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
        DPRINTF(DEBUG_CAS_Server, "something bad happened\n");
        assert(false);
        return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
    }
}

std::string CAS_Server::put_fin(string& key, uint32_t conf_id, string& timestamp, const Request& req, Cache& cache,
        Persistent& persistent, std::mutex& lock_t){
    
    std::lock_guard <std::mutex> lock(lock_t);
    
    DPRINTF(DEBUG_CAS_Server, "started.\n");
    
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    
    DPRINTF(DEBUG_CAS_Server, "con_key is %s\n", con_key.c_str());
    
    bool fnd = cache.exists(con_key);

//    DPRINTF(DEBUG_CAS_Server,"11111.\n");
    bool flag = false;
    strVec data;
    
    if(!fnd){

//        DPRINTF(DEBUG_CAS_Server,"5555555.\n");
        fnd = persistent.exists(con_key);
        
        // If Tag not seen before, add the tuple
        if(!fnd){
//            DPRINTF(DEBUG_CAS_Server,"3333333.\n");
//            DPRINTF(DEBUG_CAS_Server,"WARNING: put_fin received but the key doesn't exist!\n");
            
            Reconfig_key_info* rki = create_rki(key, conf_id);
            if(rki == nullptr){
                return {"ERROR", "INTERNAL ERROR"};
            }
            
            //Todo: shahrooz: add previous timestamp to the end of the value
            std::vector <std::string> value{"", CAS_PROTOCOL_NAME, timestamp, rki->get_string(), "FIN"};
            
            if(rki != nullptr){
                delete rki;
            }
            
            DPRINTF(DEBUG_CAS_Server, "timestamp : %s is being written\n", timestamp.c_str());
            ///TODO:: optimize and create threads here
            // For thread, add label check here and then return
//            DPRINTF(DEBUG_CAS_Server,"Adding entries to both cache and persitent\n");
            // Also add fin tag to both
            cache.put(con_key, value);
            persistent.put(con_key, value);
            complete_fin(key, conf_id, timestamp, cache, persistent);
            flag = true;
            DPRINTF(DEBUG_CAS_Server, "finished.\n");
            fflush(stdout);
            return DataTransfer::serialize({"OK"});
        }
        else{
            DPRINTF(DEBUG_CAS_Server, "TAG found in persistent\n ");
            //TODO:: Need to insert in cache where. otherwise modify being done in next steps maybe invalid
            data = persistent.get(con_key);
        }
        
    }
    else{
//        DPRINTF(DEBUG_CAS_Server,"222222.\n");
        data = (*cache.get(con_key));
    }
    
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, timestamp, req, msg, recon_timestamp, cache, persistent);
    DPRINTF(DEBUG_CAS_Server, "recon_status is %d\n", recon_status);
    if(recon_status == 0){
        DPRINTF(DEBUG_CAS_Server, "Writing FIN.\n");
        data[4] = "FIN";
        cache.put(con_key, data);
        persistent.put(con_key, data);
        complete_fin(key, conf_id, timestamp, cache, persistent);
        DPRINTF(DEBUG_CAS_Server, "finished.\n");
        fflush(stdout);
        return DataTransfer::serialize({"OK"});
    }
    else if(recon_status == 1 && !flag){
        (*recon_keys)[key].push_back(req);
        return "SEND_NOTHING";
        // Do nothing, it will be taken care of later
    }
    else if(recon_status == 2){
        if(recon_timestamp != ""){
            Timestamp temp(recon_timestamp);
            Timestamp c_temp(timestamp);
            if(!(temp > c_temp)){
                data[4] = "FIN";
                cache.put(con_key, data);
                persistent.put(con_key, data);
                complete_fin(key, conf_id, timestamp, cache, persistent);
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
        DPRINTF(DEBUG_CAS_Server, "something bad happened\n");
        assert(false);
        return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
    }
}


//TOD0:: when fin tag with empty value inserted, how is that handled at server for future requests,
// as in won't the server respond with emoty values when requested again.
std::string CAS_Server::get(string& key, uint32_t conf_id, string& timestamp, const Request& req, Cache& cache,
        Persistent& persistent, std::mutex& lock_t){
    std::lock_guard <std::mutex> lock(lock_t);
    DPRINTF(DEBUG_CAS_Server, "started.\n");
    
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id, timestamp);
    
    //std::unique_lock<std::mutex> lock(lock_t);
    strVec result;
    
    strVec data;
    bool flag = false;
    
    bool fnd = cache.exists(con_key);
    if(!fnd){
        fnd = persistent.exists(con_key);
        if(!fnd){
            
            Reconfig_key_info* rki = create_rki(key, conf_id);
            if(rki == nullptr){
                return {"ERROR", "INTERNAL ERROR"};
            }
            
            //Todo: shahrooz: add previous timestamp to the end of the value
            std::vector <std::string> value{"", CAS_PROTOCOL_NAME, timestamp, rki->get_string(), "FIN"};
            
            if(rki != nullptr){
                delete rki;
            }
            
            DPRINTF(DEBUG_CAS_Server, "timestamp : %s is being written\n", timestamp.c_str());
            ///TODO:: optimize and create threads here
            // For thread, add label check here and then return
            DPRINTF(DEBUG_CAS_Server, "Adding entries to both cache and persitent\n");
            // Also add fin tag to both
            cache.put(con_key, value);
            persistent.put(con_key, value);
            complete_fin(key, conf_id, timestamp, cache, persistent);
            flag = true;
            DPRINTF(DEBUG_CAS_Server, "finished.\n");
            return DataTransfer::serialize({"OK", "Ack"});
//            return DataTransfer::serialize({"Failed", "None"});
        }
        else{
            data = persistent.get(con_key);
        }
    }
    else{
        data = (*cache.get(con_key));
        DPRINTF(DEBUG_CAS_Server, " GET value found in Cache \n");
    }
    
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, timestamp, req, msg, recon_timestamp, cache, persistent);
    if(recon_status == 0){
        DPRINTF(DEBUG_CAS_Server, "Writing FIN1.\n");
        data[4] = "FIN";
        DPRINTF(DEBUG_CAS_Server, "Writing FIN2.\n");
        
        cache.put(con_key, data);
        persistent.put(con_key, data);
        complete_fin(key, conf_id, timestamp, cache, persistent);
        DPRINTF(DEBUG_CAS_Server, "finished.\n");
        fflush(stdout);
        if(data[0] != ""){
            return DataTransfer::serialize({"OK", data[0]});
        }
        else{
            return DataTransfer::serialize({"OK", "Ack"});
        }
    }
    else if(recon_status == 1 && !flag){
        (*recon_keys)[key].push_back(req);
        return "SEND_NOTHING";
        // Do nothing, it will be taken care of later
    }
    else if(recon_status == 2){
        if(recon_timestamp != ""){
            Timestamp temp(recon_timestamp);
            Timestamp c_temp(timestamp);
            if(!(temp > c_temp)){
                data[4] = "FIN";
                cache.put(con_key, data);
                persistent.put(con_key, data);
                complete_fin(key, conf_id, timestamp, cache, persistent);
                
                if(data[0] != ""){
                    return DataTransfer::serialize({"OK", data[0]});
                }
                else{
                    return DataTransfer::serialize({"Failed", "None"});
                }
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
        DPRINTF(DEBUG_CAS_Server, "something bad happened\n");
        assert(false);
        return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
    }
}
