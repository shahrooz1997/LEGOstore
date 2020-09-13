/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Metadata_Server.cpp
 * Author: shahrooz
 *
 * Created on August 26, 2020, 11:17 PM
 */

#include <cstdlib>
#include "Util.h"
#include <map>
#include <string>
#include <sys/socket.h>
//#include <stdlib.h>
#include <netinet/in.h>
#include <vector>
#include <thread>
#include <mutex>
#include "Data_Transfer.h"
#include <cstdlib>
#include <utility>
#include <sstream>
#include <iostream>

// Todo: add the timestamp that the reconfiguration happened on to the confid part of the pair. confid!timestamp
//       the server may or may not need it. We will see.

using namespace std;

/*
 * 
 */

std::map<std::string, pair<string, Placement> > key_info; //key -> confid!timestamp, placement
std::mutex lock_t;

inline std::string construct_key_metadata(const std::string &key, const std::string &conf_id){
    std::string ret;
    ret += key;
    ret += "!";
    ret += conf_id;
    return ret;
}

inline std::string construct_key_metadata(const std::string &key, const uint32_t conf_id){
    return construct_key_metadata(key, to_string(conf_id));
}

uint32_t get_most_recent_conf_id(const string& key, const string& confid){
    auto it = key_info.find(construct_key_metadata(key, confid));
//    uint32_t asked_conf_id = stoul(confid);
    uint32_t saved_conf_id = stoul(it->second.first);
    
    if(saved_conf_id == stoul(confid))
        return saved_conf_id;
    
    while(true){
        auto it2 = key_info.find(construct_key_metadata(key, saved_conf_id));
        if(saved_conf_id == stoul(it2->second.first))
            break;
        saved_conf_id = stoul(it2->second.first);
    }
    
    return saved_conf_id;
}

inline std::string construct_confid_timestamp(const std::string& confid, const std::string& timestamp){
    return confid + "!" + timestamp;
}

string ask(const string& key, const string& confid){ // respond with (requested_confid!new_confid!timestamp, p)
    
    DPRINTF(DEBUG_METADATA_SERVER, "key: %s, confid: %s\n", key.c_str(), confid.c_str());
    
    if(confid == "0"){ // exception case.
        auto it = key_info.find(construct_key_metadata(key, confid));
        
        uint32_t saved_conf_id = stoul(it->second.first.substr(0, it->second.first.find("!")));
        
        auto it2 = key_info.find(construct_key_metadata(key, saved_conf_id));
        return DataTransfer::serializeMDS("OK", to_string(saved_conf_id) + "!" + it->second.first, &(it2->second.second));
    }
    
    auto it = key_info.find(construct_key_metadata(key, confid));
    
    if(it == key_info.end()){ // Not found!
        return DataTransfer::serializeMDS("ERROR", "key with that conf_id does not exist", nullptr);
    }
    
    
    return DataTransfer::serializeMDS("OK", confid + "!" + it->second.first, &(it->second.second));
}

string update(const string& key, const string& old_confid, const string& new_confid, const string& timestamp, Placement* p){
    auto it = key_info.find(construct_key_metadata(key, old_confid));
    
    DPRINTF(DEBUG_METADATA_SERVER, "key: %s, old_conf: %s, new_conf: %s\n", key.c_str(), old_confid.c_str(), new_confid.c_str());
    
    if(it == key_info.end()){ // Not found!
        if(old_confid != new_confid){
            return DataTransfer::serializeMDS("ERROR", "key does not exist and cannot be initialized", nullptr);
        }
        key_info[construct_key_metadata(key, 0)] = std::pair<string, Placement>(construct_confid_timestamp(old_confid, "NULL"), *p);
        key_info[construct_key_metadata(key, old_confid)] = std::pair<string, Placement>(construct_confid_timestamp(new_confid, "NULL"), *p);
        return DataTransfer::serializeMDS("WARN", "key does not exist, initialization...", nullptr);
    }
    
    if(timestamp.find("-") >= timestamp.size()){
        assert(false);
    }
    
    it->second.first = construct_confid_timestamp(new_confid, timestamp); // reconfiguration happened on old_confid to new_confid with timestamp timestamp
    
    key_info[construct_key_metadata(key, new_confid)] = pair<string, Placement>(construct_confid_timestamp(new_confid, "NULL"), *p);
    
    return DataTransfer::serializeMDS("OK", "key updated", nullptr);
}

void server_connection(int connection, int portid){

    std::string recvd;
    int result = DataTransfer::recvMsg(connection,recvd);
    if(result != 1){
        DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Error in receiving"));
        close(connection);
        return;
    }

    string status;
    string msg;
    
    Placement *p = DataTransfer::deserializeMDS(recvd, status, msg);
    std::string &method = status; // Method: ask/update, key, conf_id
    
    
    std::unique_lock<std::mutex> lock(lock_t);
    if(method == "ask"){
        DPRINTF(DEBUG_METADATA_SERVER, "The method ask is called. The msg is %s, server port is %u\n",
                    msg.c_str(), portid);
        std::size_t pos = msg.find("!");
        if(pos >= msg.size()){
            std::stringstream pmsg; // thread safe printing
            pmsg << "BAD FORMAT: " << msg << std::endl;
            std::cerr << pmsg.str();
            assert(0);
        }
	string key = msg.substr(0, pos);
	string conf_id = msg.substr(pos + 1);
        result = DataTransfer::sendMsg(connection, ask(key, conf_id));
    }else if(method == "update"){
        DPRINTF(DEBUG_METADATA_SERVER, "The method update is called. The msg is %s, server port is %u\n",
                    msg.c_str(), portid);
        std::size_t pos = msg.find("!");
        std::size_t pos2 = msg.find("!", pos + 1);
        std::size_t pos3 = msg.find("!", pos2 + 1);
        if(pos >= msg.size() || pos2 >= msg.size() || pos3 >= msg.size()){
            std::stringstream pmsg; // thread safe printing
            pmsg << "BAD FORMAT: " << msg << std::endl;
            std::cerr << pmsg.str();
            assert(0);
        }
        string key = msg.substr(0, pos);
	    string old_confid = msg.substr(pos + 1, pos2 - pos - 1);
        string new_confid = msg.substr(pos2 + 1, pos3 - pos2 - 1);
        string timestamp = msg.substr(pos3 + 1);
        
        result = DataTransfer::sendMsg(connection, update(key, old_confid, new_confid, timestamp, p));
    }
    else {
            DataTransfer::sendMsg(connection,  DataTransfer::serializeMDS("ERROR", "Unknown method is called"));
    }

    if(result != 1){
            DataTransfer::sendMsg(connection,  DataTransfer::serializeMDS("ERROR", "Server Response failed"));
    }

    shutdown(connection, SHUT_WR);
    close(connection);
}

void runServer(const std::string &socket_port){
    
    int sock = socket_setup(socket_port);   
    DPRINTF(DEBUG_METADATA_SERVER, "Alive port is %s\n", socket_port.c_str());
    
    while(1){
        int portid = stoi(socket_port);
        int new_sock = accept(sock, NULL, 0);
        std::thread cThread([new_sock, portid](){ server_connection(new_sock, portid);});
        cThread.detach();
    }
    
    close(sock);
}

int main(int argc, char** argv) {
    
//    std::cout << "AAAA" << std::endl;
    runServer(METADATA_SERVER_PORT);

    return 0;
}

