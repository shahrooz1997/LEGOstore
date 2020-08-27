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

ABD_Server::ABD_Server() {
}

ABD_Server::~ABD_Server() {
}

std::string ABD_Server::get_timestamp(string &key, uint32_t conf_id, Cache &cache, Persistent &persistent, std::mutex &lock_t){

    DPRINTF(DEBUG_ABD_Server, "get_timestamp started and the key is %s\n", key.c_str());
    const std::vector<std::string> *ptr = cache.get(key);
    strVec data;
    strVec result;

    if(ptr == nullptr){
            data = persistent.get(key);
            DPRINTF(DEBUG_ABD_Server,"cache data checked and data is null\n");
            if(data.empty()){
                    result = {"Failed", "None", "None"};
            }else{
                DPRINTF(DEBUG_ABD_Server,"Found data in persistent. sending back! data is"\
                        "key:%s   timestamp:%s, value: %s\n", key.c_str(), data[1].c_str(), data[0].c_str());
                result = {"OK", data[0], data[1]};
            }
    }else{
//            DPRINTF(DEBUG_ABD_Server,"cache data checked and data is found\n");
            data = (*ptr);
//            DPRINTF(DEBUG_ABD_Server,"cache data checked and data is %s\n",data[1].c_str());
            result = {"OK", data[0], data[1]};
            DPRINTF(DEBUG_ABD_Server,"Found data in cache. sending back! data is"\
                            "key:%s   timestamp:%s, value: %s\n", key.c_str(), data[1].c_str(), data[0].c_str());
    }

    assert(result.empty() != 1);
    return DataTransfer::serialize(result);
}


std::string ABD_Server::put(string &key, string &value, string &timestamp, Cache &cache, Persistent &persistent, std::mutex &lock_t){

    //std::lock_guard<std::mutex> lock(lock_t);
    DPRINTF(DEBUG_ABD_Server,"put function timestamp %s\n", timestamp.c_str());
    const std::vector<std::string> *ptr = cache.get(key);
    strVec data;
    strVec result;

    if(ptr == nullptr){
        cache.put(key, {value, timestamp});
        persistent.put(key, {value, timestamp});
    }else{
        data = (*ptr);
        Timestamp curr_time(timestamp);
    	Timestamp saved_time(data[1]);

        if(curr_time > saved_time){
            cache.put(key, {value, timestamp});
            persistent.put(key, {value, timestamp});
        }
    }

	return DataTransfer::serialize({"OK"});
}


std::string ABD_Server::get(string &key, string &timestamp, Cache &cache, Persistent &persistent, std::mutex &lock_t){

    //std::unique_lock<std::mutex> lock(lock_t);
    DPRINTF(DEBUG_ABD_Server,"GET function called !! \n");
    const std::vector<std::string> *ptr = cache.get(key);
    strVec data;
    strVec result;

    if(ptr == nullptr){
        data = persistent.get(key);
        DPRINTF(DEBUG_ABD_Server,"cache data checked and data is null\n");
        if(data.empty()){
            result = {"Failed", "None"};
        }else{
            result = {"OK", data[0], data[1]};
        }
    }else{
        DPRINTF(DEBUG_ABD_Server,"cache data checked and data is found\n");
        data = (*ptr);
        result = {"OK", data[0], data[1]};
    }

    assert(result.empty() != 1);
    return DataTransfer::serialize(result);
}
