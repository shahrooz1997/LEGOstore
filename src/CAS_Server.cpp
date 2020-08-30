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

CAS_Server::CAS_Server() {
}


CAS_Server::~CAS_Server() {
}

// Returns true if success, i.e., timestamp added as the fin timestamp for the key
bool complete_fin(std::string &key, uint32_t conf_id, std::string &timestamp, Cache &cache, Persistent &persistent){

    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
    
    strVec data;
    bool fnd = cache.exists(con_key);
    if(!fnd){
        fnd = persistent.exists(con_key);
        if(!fnd){
            cache.put(con_key, {timestamp});
            persistent.put(con_key, {timestamp});
            return true;
        }
        data = persistent.get(con_key);
    }else{
        data = *cache.get(con_key);
    }
    
    return true;
}

int CAS_Server::reconfig_info(const string &key, uint32_t conf_id, string &timestamp, const Request &req, string &msg, string &recon_timestamp, Cache &cache, Persistent &persistent){
    DPRINTF(DEBUG_CAS_Server, "started.\n");
    
    int ret = 0;
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id, &timestamp);
    
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
            recon_timestamp = rki.timestamp;
            ret = 1;
        }
        else if(rki.reconfig_state == 2){ // Reconfigured
            recon_timestamp = rki.timestamp;
            msg = std::to_string(rki.next_conf_id);
            ret = 2;
        }
        else{
            ret = -3;
        }
    }
    
//    DPRINTF(DEBUG_CAS_Server,"recon_status is %d\n", ret);

    DPRINTF(DEBUG_CAS_Server, "finished.\n");
    return ret;
}

std::string CAS_Server::get_timestamp(const string &key, uint32_t conf_id, const Request &req, Cache &cache, Persistent &persistent, std::mutex &lock_t){

    std::lock_guard<std::mutex> lock(lock_t);
    DPRINTF(DEBUG_CAS_Server,"started.\n");
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
    
    DPRINTF(DEBUG_CAS_Server, "get_timestamp started and the key is %s\n", con_key.c_str());
    const std::vector<std::string> *ptr = cache.get(con_key);
    strVec data; // = (*cache.get(key))[0];
    strVec result;

    if(ptr == nullptr){
        data = persistent.get(con_key);
        DPRINTF(DEBUG_CAS_Server,"cache data checked and data is null\n");
        if(data.empty()){
            result = {"Failed", "None"};
            return DataTransfer::serialize(result);
        }else{
            DPRINTF(DEBUG_CAS_Server,"Found data in persisten. sending back! data is"\
                    "key:%s   timestamp:%s\n", con_key.c_str(), data[0].c_str() );
        }
    }
    else{
        data = (*ptr);
        DPRINTF(DEBUG_CAS_Server,"cache data checked and data is %s\n",data[0].c_str());
    }

    // Data is found
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, data[0], req, msg, recon_timestamp, cache, persistent);
    
    if(recon_status == 0){
        result = {"OK", data[0]};
        return DataTransfer::serialize(result);
    }
    else if(recon_status == 1){
        recon_keys[key].push_back(req);
        return "SEND_NOTHING";
        // Do nothing, it will be taken care of later
    }
    else if(recon_status == 2){
        result = {"operation_fail", msg};
        return DataTransfer::serialize(result);
    }
    else{
        DPRINTF(DEBUG_CAS_Server,"something bad happened\n");
        assert(false);
        result = {"ERROR", "INTERNAL ERROR"};
        return DataTransfer::serialize(result);
    }
}

std::string CAS_Server::put(string &key, uint32_t conf_id, string &val, string &timestamp, const Request &req, Cache &cache, Persistent &persistent, std::mutex &lock_t){

    std::lock_guard<std::mutex> lock(lock_t);
    DPRINTF(DEBUG_CAS_Server,"started.\n");
//	DPRINTF(DEBUG_CAS_Server,"put function timestamp %s\n", timestamp.c_str());
//	insert_data(key, conf_id, value, timestamp, false, cache, persistent);
//	return DataTransfer::serialize({"OK"});
    
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id, &timestamp);
    DPRINTF(DEBUG_CAS_Server,"con_key is %s\n", con_key.c_str());
    
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
                
                Reconfig_key_info rki;
                rki.curr_conf_id = conf_id;
                rki.curr_placement = nullptr;
                rki.reconfig_state = 0;
                rki.timestamp = ""; // Todo: maybe you need to add it into the metadata server
                rki.next_conf_id = -1;
                rki.next_placement = nullptr;
                
                //Todo: shahrooz: get placement info from metadata server
                Connect c(METADATA_SERVER_IP, METADATA_SERVER_PORT);
                if(!c.is_connected()){
                    return DataTransfer::serialize({"ERROR", "Metadata Server is down"});
                }
                DataTransfer::sendMsg(*c,DataTransfer::serializeMDS("ask", key + "!" + std::to_string(conf_id)));
                std::string recvd;
                if(DataTransfer::recvMsg(*c, recvd) == 1){
                    std::string status;
                    std::string msg;
                    rki.curr_placement =  DataTransfer::deserializeMDS(recvd, status, msg);
                    if(status == "OK"){
                        if(stoul(msg) > conf_id){
                            rki.reconfig_state = 2;
                            rki.next_conf_id = stoul(msg);
                            // Todo: we may need to fill next_placement and timestamp fields too
                        }
                    }
                }
                
                //Todo: shahrooz: add previous timestamp to the end of the value
                std::vector<std::string> value{val, CAS_PROTOCOL_NAME, timestamp, rki.get_string(), "PRE"};

                DPRINTF(DEBUG_CAS_Server,"timestamp : %s is being written\n", timestamp.c_str());
                ///TODO:: optimize and create threads here
                // For thread, add label check here and then return
//                DPRINTF(DEBUG_CAS_Server,"Adding entries to both cache and persitent\n");
                // Also add fin tag to both
                cache.put(con_key, value);
                persistent.put(con_key, value);
                flag = true;
//                return DataTransfer::serialize({"OK"}); // Todo: make sure that it is correct to comment out this line
            }
            else{
                assert(false);
            }
        }
        else{
            DPRINTF(DEBUG_CAS_Server,"key found in persistent\n");
            //TODO:: Need to insert in cache where. otherwise modify being done in next steps maybe invalid
        }
    }
    
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, timestamp, req, msg, recon_timestamp, cache, persistent);
    DPRINTF(DEBUG_CAS_Server,"recon_status is %d\n", recon_status);
    if(recon_status == 0){
        return DataTransfer::serialize({"OK"});
    }
    else if(recon_status == 1 && !flag){
        recon_keys[key].push_back(req);
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
        DPRINTF(DEBUG_CAS_Server,"something bad happened\n");
        assert(false);
        return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
    }
}

std::string CAS_Server::put_fin(string &key, uint32_t conf_id, string &timestamp, const Request &req, Cache &cache, Persistent &persistent, std::mutex &lock_t){

    std::lock_guard<std::mutex> lock(lock_t);
    
    DPRINTF(DEBUG_CAS_Server,"started.\n");

    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id, &timestamp);
    
    DPRINTF(DEBUG_CAS_Server,"con_key is %s\n", con_key.c_str());

    bool fnd = cache.exists(con_key);

    DPRINTF(DEBUG_CAS_Server,"11111.\n");
    bool flag = false;
    strVec data;
    
    if(!fnd){
        
        DPRINTF(DEBUG_CAS_Server,"5555555.\n");
        fnd = persistent.exists(con_key);

        // If Tag not seen before, add the tuple
        if(!fnd){
            DPRINTF(DEBUG_CAS_Server,"3333333.\n");
//            DPRINTF(DEBUG_CAS_Server,"WARNING: put_fin received but the key doesn't exist!\n");
            
            Reconfig_key_info rki;
            rki.curr_conf_id = conf_id;
            rki.curr_placement = nullptr;
            rki.reconfig_state = 0;
            rki.timestamp = ""; // Todo: maybe you need to add it into the metadata server
            rki.next_conf_id = -1;
            rki.next_placement = nullptr;

            //Todo: shahrooz: get placement info from metadata server
            Connect c(METADATA_SERVER_IP, METADATA_SERVER_PORT);
            if(!c.is_connected()){
                return DataTransfer::serialize({"ERROR", "Metadata Server is down"});
            }
            DataTransfer::sendMsg(*c,DataTransfer::serializeMDS("ask", key + "!" + std::to_string(conf_id)));
            std::string recvd;
            if(DataTransfer::recvMsg(*c, recvd) == 1){
                std::string status;
                std::string msg;
                rki.curr_placement =  DataTransfer::deserializeMDS(recvd, status, msg);
                if(status == "OK"){
                    if(stoul(msg) > conf_id){
                        rki.reconfig_state = 2;
                        rki.next_conf_id = stoul(msg);
                        // Todo: we may need to fill next_placement and timestamp fields too
                    }
                }
            }

            //Todo: shahrooz: add previous timestamp to the end of the value
            std::vector<std::string> value{"", CAS_PROTOCOL_NAME, timestamp, rki.get_string(), "FIN"};

            DPRINTF(DEBUG_CAS_Server,"timestamp : %s is being written\n", timestamp.c_str());
            ///TODO:: optimize and create threads here
            // For thread, add label check here and then return
//            DPRINTF(DEBUG_CAS_Server,"Adding entries to both cache and persitent\n");
            // Also add fin tag to both
            cache.put(con_key, value);
            persistent.put(con_key, value);
            complete_fin(key, conf_id, timestamp, cache, persistent);
            flag = true;
//            return DataTransfer::serialize({"OK"});
        }
        else{
            DPRINTF(DEBUG_CAS_Server,"TAG found in persistent\n ");
            //TODO:: Need to insert in cache where. otherwise modify being done in next steps maybe invalid
            data = persistent.get(con_key);
        }
        
    }
    else{
        DPRINTF(DEBUG_CAS_Server,"222222.\n");
        data = (*cache.get(con_key));
    }
    
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, timestamp, req, msg, recon_timestamp, cache, persistent);
    DPRINTF(DEBUG_CAS_Server,"recon_status is %d\n", recon_status);
    fflush(stdout);
    if(recon_status == 0){
        data[4] = "FIN";
        cache.put(con_key, data);
        persistent.put(con_key, data);
        complete_fin(key, conf_id, timestamp, cache, persistent);
        return DataTransfer::serialize({"OK"});
    }
    else if(recon_status == 1 && !flag){
        recon_keys[key].push_back(req);
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
        DPRINTF(DEBUG_CAS_Server,"something bad happened\n");
        assert(false);
        return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
    }
}



//TOD0:: when fin tag with empty value inserted, how is that handled at server for future requests,
// as in won't the server respond with emoty values when requested again.
std::string CAS_Server::get(string &key, uint32_t conf_id, string &timestamp, const Request &req, Cache &cache, Persistent &persistent, std::mutex &lock_t){
    std::lock_guard<std::mutex> lock(lock_t);
    
    std::string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id, &timestamp);
    
    //std::unique_lock<std::mutex> lock(lock_t);
    DPRINTF(DEBUG_CAS_Server,"GET function called !! \n");
    strVec result;
    
    strVec data;
    bool flag = false;

    bool fnd = cache.exists(con_key);
    if(!fnd){
        fnd = persistent.exists(con_key);
        if(!fnd){
            
            Reconfig_key_info rki;
            rki.curr_conf_id = conf_id;
            rki.curr_placement = nullptr;
            rki.reconfig_state = 0;
            rki.timestamp = ""; // Todo: maybe you need to add it into the metadata server
            rki.next_conf_id = -1;
            rki.next_placement = nullptr;

            //Todo: shahrooz: get placement info from metadata server
            Connect c(METADATA_SERVER_IP, METADATA_SERVER_PORT);
            if(!c.is_connected()){
                return DataTransfer::serialize({"ERROR", "Metadata Server is down"});
            }
            DataTransfer::sendMsg(*c,DataTransfer::serializeMDS("ask", key + "!" + std::to_string(conf_id)));
            std::string recvd;
            if(DataTransfer::recvMsg(*c, recvd) == 1){
                std::string status;
                std::string msg;
                rki.curr_placement =  DataTransfer::deserializeMDS(recvd, status, msg);
                if(status == "OK"){
                    if(stoul(msg) > conf_id){
                        rki.reconfig_state = 2;
                        rki.next_conf_id = stoul(msg);
                        // Todo: we may need to fill next_placement and timestamp fields too
                    }
                }
            }

            //Todo: shahrooz: add previous timestamp to the end of the value
            std::vector<std::string> value{"", CAS_PROTOCOL_NAME, timestamp, rki.get_string(), "FIN"};

            DPRINTF(DEBUG_CAS_Server,"timestamp : %s is being written\n", timestamp.c_str());
            ///TODO:: optimize and create threads here
            // For thread, add label check here and then return
            DPRINTF(DEBUG_CAS_Server,"Adding entries to both cache and persitent\n");
            // Also add fin tag to both
            cache.put(con_key, value);
            persistent.put(con_key, value);
            complete_fin(key, conf_id, timestamp, cache, persistent);
            flag = true;
//                return DataTransfer::serialize({"OK"});
//            return DataTransfer::serialize({"Failed", "None"});
        }
        else{
            data = persistent.get(con_key);
        }
    }
    else{
        data = (*cache.get(con_key));
        DPRINTF(DEBUG_CAS_Server," GET value found in Cache \n");
    }
    
    std::string msg;
    std::string recon_timestamp;
    int recon_status = reconfig_info(key, conf_id, timestamp, req, msg, recon_timestamp, cache, persistent);
    if(recon_status == 0){
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
    else if(recon_status == 1 && !flag){
        recon_keys[key].push_back(req);
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
        DPRINTF(DEBUG_CAS_Server,"something bad happened\n");
        assert(false);
        return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
    }
}
