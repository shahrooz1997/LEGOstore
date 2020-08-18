/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   ABD_Client.cpp
 * Author: shahrooz
 * 
 * Created on August 14, 2020, 3:07 PM
 */

#include "ABD_Client.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <regex>
#include <future>
#include <sys/time.h>

namespace ABD_thread_helper{
    void _get_timestamp(std::promise<strVec> &&prm, std::string key, Server *server,
                        std::string current_class){

        DPRINTF(DEBUG_ABD_Client, "started.\n");

        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            return;
        }

        strVec data;
        data.push_back("get_timestamp");
        data.push_back(key);
        data.push_back(current_class);
        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        data.clear();
        std::string recvd;

        // If the socket recv itself fails, then 'promise' value will not be made available
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data =  DataTransfer::deserialize(recvd);
            prm.set_value(std::move(data));
        }
        
        DPRINTF(DEBUG_ABD_Client, "finished.\n");

        return;
    }
    
    void _put(std::promise<strVec> &&prm, std::string key, std::string value, Timestamp timestamp,
                        Server *server, std::string current_class){

        DPRINTF(DEBUG_ABD_Client, "started.\n");

        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            return;
        }

        strVec data;
        data.push_back("put");
        data.push_back(key);
        data.push_back(timestamp.get_string());
        data.push_back(value);
        data.push_back(current_class);

        if((value).empty()){
                   printf("WARNING!!! SENDING EMPTY STRING TO SERVER\n");
        }
        DataTransfer::sendMsg(*c,DataTransfer::serialize(data));

        data.clear();
        std::string recvd;


        // If the socket recv itself fails, then 'promise' value will not be made available
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data =  DataTransfer::deserialize(recvd);
            prm.set_value(std::move(data));
        }

        DPRINTF(DEBUG_ABD_Client, "finished with server port: %u\n", server->port);

        return;
    }
}

ABD_Client::ABD_Client(Properties *prop, uint32_t client_id, Placement &pp) {
    this->current_class = ABD_PROTOCOL_NAME;
    this->id = client_id;
    this->prop = prop;
    this->p = pp;
    
//    this->operation_id = 0;
    
#ifdef LOGGING_ON
    char name[20];
    name[0] = 'l';
    name[1] = 'o';
    name[2] = 'g';
    name[3] = 's';
    name[4] = '/';

    sprintf(&name[5], "%u.txt", client_id);
    this->log_file = fopen(name, "w");
#endif
}

ABD_Client::~ABD_Client() {
#ifdef LOGGING_ON
    fclose(this->log_file);
#endif
    
    DPRINTF(DEBUG_ABD_Client, "cliend with id \"%u\" has been destructed.\n", this->id);
}

void ABD_Client::update_placement(std::string &new_cfg){
    this->p = DataTransfer::deserializeCFG(new_cfg);
}

uint32_t ABD_Client::get_timestamp(std::string *key, Timestamp **timestamp, std::string &value){

    DPRINTF(DEBUG_ABD_Client, "started.\n");

    std::vector<Timestamp> tss;
    std::vector<std::string> vs;
    *timestamp = nullptr;
    std::vector<std::future<strVec> > responses;
    int retries = this->prop->retry_attempts;
    bool op_status = false;    //Success

    while(!op_status && retries--){
        for(auto it = p.Q1.begin();it != p.Q1.end(); it++){

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_get_timestamp, std::move(prm), *key,
                            prop->datacenters[*it]->servers[0], this->current_class).detach();
        }

        for(auto &it:responses){
            strVec data = it.get();

            if(data[0] == "OK"){
                printf("get timestamp for key :%s, ts: %s, value: %s\n", key->c_str(), data[2].c_str(), data[1].c_str());
                
                vs.emplace_back(data[1]);
                tss.emplace_back(data[2]);
                op_status = true;   // For get_timestamp, even if one response Received
                                    // operation is success
            }else if(data[0] == "operation_fail"){
                update_placement(data[1]);
                op_status = false;      // Retry the operation
                printf("get_timestamp : operation failed received! for key : %s", key->c_str());
                return S_RECFG;
                //break;
            }
            //else : The server returned "Failed", that means the entry was not found
            // We ignore the response in that case.
            // The servers can return Failed for timestamp, which is acceptable
        }

        responses.clear();
    }

    if(op_status){
        
        uint32_t max_ts_i = Timestamp::max_timestamp3(tss);
        *timestamp = new Timestamp(tss[max_ts_i]);
        value = vs[max_ts_i];
        
        DPRINTF(DEBUG_ABD_Client, "finished successfully.\n");
    }else{
        DPRINTF(DEBUG_ABD_Client, "Operation Failed.\n");
        return S_FAIL;
    }

    return S_OK;
}

uint32_t ABD_Client::put(std::string key, std::string value, bool insert){

#ifdef LOGGING_ON
    gettimeofday (&log_tv, NULL);
    log_tm = localtime (&log_tv.tv_sec);
    strftime (log_fmt, sizeof (log_fmt), "%H:%M:%S:%%06u", log_tm);
    snprintf (log_buf, sizeof (log_buf), log_fmt, log_tv.tv_usec);
    fprintf(this->log_file, "%s write invoke %s\n", log_buf, value.c_str());
#endif
    
    key = std::string("ABD" + key);

    int retries = this->prop->retry_attempts;
    bool op_status = false;

    while(!op_status && retries--){
        
        op_status = true;

        DPRINTF(DEBUG_ABD_Client, "The value to be stored is %s \n", value.c_str());

        Timestamp *timestamp = nullptr;
        std::string rec_value;
        Timestamp *tmp = nullptr;
//        int status = S_OK;
        std::vector<std::future<strVec> > responses;

        if(insert){ // This is a new key
            timestamp = new Timestamp(this->id);
        }
        else{
            this->get_timestamp(&key, &tmp, rec_value);

            if(tmp != nullptr){
                timestamp = new Timestamp(tmp->increase_timestamp(this->id));
                delete tmp;
            }
        }

        if(timestamp == nullptr){

            DPRINTF(DEBUG_ABD_Client, "get_timestamp operation failed key %s \n", key.c_str());
            op_status = false;
            continue;
        }


        // put
        DPRINTF(DEBUG_ABD_Client, "Issue Q2 request to key: %s and timestamp: %s  value :%s ", key.c_str(), timestamp->get_string().c_str(), value.c_str());
        int i = 0;

        for(auto it = p.Q2.begin(); it != p.Q2.end(); it++){
            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_put, std::move(prm), key, value, *timestamp,
                                prop->datacenters[*it]->servers[0], this->current_class).detach();

            i++;
        }

        for(auto &it:responses){
            strVec data = it.get();

            if(data[0] == "OK"){

            } else{
                delete timestamp;
                if(data[0] == "operation_fail"){
                    update_placement(data[1]);
                }
                op_status = false;
                printf("_put operation failed for key : %s   %s \n", key.c_str(), data[0].c_str());
                break;
            }
        }

        responses.clear();

    }

    return op_status? S_OK : S_FAIL;
}

uint32_t ABD_Client::get(std::string key, std::string &value){

#ifdef LOGGING_ON
    gettimeofday (&log_tv, NULL);
    log_tm = localtime (&log_tv.tv_sec);
    strftime (log_fmt, sizeof (log_fmt), "%H:%M:%S:%%06u", log_tm);
    snprintf (log_buf, sizeof (log_buf), log_fmt, log_tv.tv_usec);
    fprintf(this->log_file, "%s read invoke nil\n", log_buf);
#endif
    
    key = std::string("ABD" + key);

    value.clear();

    int retries = this->prop->retry_attempts;
    bool op_status = false;

    while(!op_status && retries--){

        Timestamp *timestamp = nullptr;
        std::string rec_value;
        this->get_timestamp(&key, &timestamp, rec_value);
        op_status = true;

        if(timestamp == nullptr){
            DPRINTF(DEBUG_ABD_Client, "get_timestamp operation failed on key %s \n", key.c_str());
            op_status = false;
            continue;
        }

        // phase 2
        std::vector <std::future<strVec> > responses;

        for(auto it = p.Q2.begin(); it != p.Q2.end(); it++){
            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_put, std::move(prm), key, rec_value, *timestamp,
                    prop->datacenters[*it]->servers[0], this->current_class).detach();

            DPRINTF(DEBUG_ABD_Client, "Issue Q2 request for key :%s \n", key.c_str());
        }

        delete timestamp;


        for(auto &it:responses){
            strVec data = it.get();

            if(data[0] == "OK"){
                
            }else if(data[0] == "operation_fail"){

                update_placement(data[1]);
                
                op_status = false;
                printf("get operation failed for key:%s  %s\n", key.c_str(), data[0].c_str());
                break;
            }
        }

        responses.clear();

        if(!op_status){
            continue;
        }
        
        value = rec_value;
    }
    
    return op_status? S_OK: S_FAIL;
}

