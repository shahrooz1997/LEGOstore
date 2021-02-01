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
#include "../inc/Data_Transfer.h"
#include <cstdlib>
#include <utility>
#include <sstream>
#include <iostream>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

// Todo: add the timestamp that the reconfiguration happened on to the confid part of the pair. confid!timestamp
//       the server may or may not need it. We will see.

using namespace std;

/*
 * KEY = key!confid
 * VALUE = pair of (newconfid!timestamp, placement)
 * if no reconfiguration has happened on key!confid, newconfid will be confid and timestamp will be ""
 */

struct Value{
    uint32_t newconfid;
    std::string timestamp; // reconfiguration happened on old_confid to new_confid with timestamp timestamp
    Placement placement;
};

std::map<std::string, Value> key_info;
std::mutex lock_t;

// Storing states of configurations
std::map<std::string, vector<std::string>> secondary_configs; 

inline std::string construct_key_metadata(const std::string& key, const std::string& conf_id){
    std::string ret;
    ret += key;
    ret += "!";
    ret += conf_id;
    return ret;
}
inline std::string construct_key_metadata(const std::string& key, const uint32_t conf_id){
    return construct_key_metadata(key, to_string(conf_id));
}
inline std::string construct_confid_timestamp(const std::string& confid, const std::string& timestamp){
    return confid + "!" + timestamp;
}

string ask(const string& key, const string& confid){ // respond with (requested_confid!new_confid!timestamp, p)
    DPRINTF(DEBUG_METADATA_SERVER, "key: %s, confid: %s\n", key.c_str(), confid.c_str());
   
    auto it1 = secondary_configs.find(key);
    string sec_configs = "";
    if(it1 != secondary_configs.end()){
        for (auto i = it1->second.begin(); i != it1->second.end(); ++i) {
            if(*i != confid) sec_configs += *i;
            if(i != it1->second.end()) sec_configs += "!"; 
        }
    }
    
    if(confid == "0"){ // exception case.
        auto it = key_info.find(construct_key_metadata(key, confid));

        uint32_t saved_conf_id = it->second.newconfid; //stoui(it->second.first.substr(0, it->second.first.find("!")));

        auto it2 = key_info.find(construct_key_metadata(key, saved_conf_id));
        return DataTransfer::serializeMDS("OK", "", key, saved_conf_id, it2->second.newconfid, it2->second.timestamp, sec_configs, it2->second.placement);
    }
    
    auto it = key_info.find(construct_key_metadata(key, confid));
    
    if(it == key_info.end()){ // Not found!
        return DataTransfer::serializeMDS("ERROR", "key with that conf_id does not exist");
    }

    return DataTransfer::serializeMDS("OK", "", key, stoui(confid), it->second.newconfid, it->second.timestamp, sec_configs, it->second.placement);
}

string
update_config_state(const string& key, const string& old_confid, const string& op, const string& timestamp){
    auto it = secondary_configs.find(key);
    string op_add = "add";   
    string op_remove = "remove";   
 
    DPRINTF(DEBUG_METADATA_SERVER, "key: %s, old_conf: %s\n", key.c_str(), old_confid.c_str());
    if(it == secondary_configs.end()){ // Not found!
        secondary_configs[key] = {};
    }

    if(timestamp.find("-") >= timestamp.size()){
        assert(false);
    }
    
    if(op == op_add && std::find(secondary_configs[key].begin(), 
            secondary_configs[key].end(), old_confid) == secondary_configs[key].end()) { 
        secondary_configs[key].push_back(old_confid);
    } else if (op == op_remove) {
        secondary_configs[key].erase(std::remove(secondary_configs[key].begin(), secondary_configs[key].end(),
                                old_confid), secondary_configs[key].end());
    }

    // Checking secondary_configs_list
    std::cout << "OP: " << op << " Secondary configs list for the given key: " << endl;
    for(auto it1 = secondary_configs[key].begin(); it1 != secondary_configs[key].end(); it1++) {
        cout << *it1 << " ";
    }
    std::cout << endl;
    return DataTransfer::serializeMDS("OK", "state updated");
}

string update(const string& key, const string& old_confid, const string& new_confid, const string& timestamp, const Placement& p){
    DPRINTF(DEBUG_METADATA_SERVER, "started. key: %s, old_confid: %s\n", key.c_str(), old_confid.c_str());
    auto it = key_info.find(construct_key_metadata(key, old_confid));
    
    if(it == key_info.end()){ // Not found!
        if(old_confid != new_confid){
            DPRINTF(DEBUG_METADATA_SERVER, "key %s does not exist and cannot be initialized\n", key.c_str());
            return DataTransfer::serializeMDS("ERROR", "key does not exist and cannot be initialized");
        }
        key_info[construct_key_metadata(key, 0)].newconfid = stoui(old_confid);
        key_info[construct_key_metadata(key, 0)].timestamp = "";
        key_info[construct_key_metadata(key, 0)].placement = Placement();
//        key_info[construct_key_metadata(key, 0)] = std::pair<string, Placement>(
//                construct_confid_timestamp(old_confid, "NULL"), *p);


//        key_info[construct_key_metadata(key, old_confid)] = std::pair<string, Placement>(
//                construct_confid_timestamp(new_confid, "NULL"), *p);
//        return DataTransfer::serializeMDS("WARN", "key does not exist, initialization...", nullptr);
    }
    else{
        if(old_confid == new_confid){
            return DataTransfer::serializeMDS("WARN", "repetitive message");
        }
    }

    if(timestamp.find("-") == string::npos && timestamp.size() != 0){
        assert(false);
    }

    key_info[construct_key_metadata(key, old_confid)].newconfid = stoui(new_confid);
    key_info[construct_key_metadata(key, old_confid)].timestamp = timestamp;

    key_info[construct_key_metadata(key, new_confid)].newconfid = stoui(new_confid);
    key_info[construct_key_metadata(key, new_confid)].timestamp = "";
    key_info[construct_key_metadata(key, new_confid)].placement = p;

    DPRINTF(DEBUG_METADATA_SERVER, "key %s with conf %s updated to conf %s and timestamp \"%s\":\n", key.c_str(),
            old_confid.c_str(), new_confid.c_str(), timestamp.c_str());
    return DataTransfer::serializeMDS("OK", "key updated");
}

void message_handler(int connection, int portid, const std::string& recvd){
    std::string status;
    std::string msg;
    std::string key;
    uint32_t curr_conf_id;
    uint32_t new_conf_id;
    std::string timestamp;
    std::string secondary_configs;
    int result = 1;
    
    Placement p = DataTransfer::deserializeMDS(recvd, status, msg, key, curr_conf_id, new_conf_id, timestamp, secondary_configs);
    std::string& method = status; // Method: ask/update, key, conf_id

    std::unique_lock <std::mutex> lock(lock_t);
    if(method == "ask"){
        result = DataTransfer::sendMsg(connection, ask(key, to_string(curr_conf_id)));
    }
    else if(method == "state"){
        DPRINTF(DEBUG_METADATA_SERVER, "The method state is called. The msg is %s, server port is %u\n", msg.c_str(),
                portid);
//        std::size_t pos = msg.find("!");
//        std::size_t pos2 = msg.find("!", pos + 1);
//        std::size_t pos3 = msg.find("!", pos2 + 1);
//
//        if(pos >= msg.size() || pos2 >= msg.size() || pos3 >= msg.size()){
//            std::stringstream pmsg; // thread safe printing
//            pmsg << "BAD FORMAT: " << msg << std::endl;
//            std::cerr << pmsg.str();
//            assert(0);
//        }
//        string key = msg.substr(0, pos);
//        string old_confid = msg.substr(pos + 1, pos2 - pos - 1);
//        string op = msg.substr(pos2 + 1, pos3 - pos2 - 1);
//        string timestamp = msg.substr(pos3 + 1);
        
        result = DataTransfer::sendMsg(connection, update_config_state(key, to_string(curr_conf_id), secondary_configs, timestamp));
    }
    else if(method == "update"){
        result = DataTransfer::sendMsg(connection, update(key, to_string(curr_conf_id), to_string(new_conf_id), timestamp, p));
    }
    else{
        DPRINTF(DEBUG_METADATA_SERVER, "Unknown method is called\n");
        DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Unknown method is called"));
    }
    
    if(result != 1){
        DPRINTF(DEBUG_METADATA_SERVER, "Server Response failed\n");
        DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Server Response failed"));
    }
    
//    shutdown(connection, SHUT_WR);
}

void server_connection(int connection, int portid){

//    int n = 1;
//    int result = setsockopt(connection, SOL_SOCKET, SO_NOSIGPIPE, &n, sizeof(n));
//    if(result < 0){
//        assert(false);
//    }

    int yes = 1;
    int result = setsockopt(connection, IPPROTO_TCP, TCP_NODELAY, (char*) &yes, sizeof(int));
    if(result < 0){
        assert(false);
    }

    while(true){
        std::string recvd;
        int result = DataTransfer::recvMsg(connection, recvd);
        if(result != 1){
//            DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Error in receiving"));
            close(connection);
            std::cout << portid << endl;
            DPRINTF(DEBUG_METADATA_SERVER, "one connection closed.\n");
            return;
        }
        if(is_warmup_message(recvd)){
//            DPRINTF(DEBUG_METADATA_SERVER, "warmup message received\n");
            std::string temp = std::string(WARM_UP_MNEMONIC) + get_random_string();
            result = DataTransfer::sendMsg(connection, temp);
            if(result != 1){
                DataTransfer::sendMsg(connection, DataTransfer::serializeMDS("ERROR", "Server Response failed"));
            }
            continue;
        }
        message_handler(connection, portid, recvd);
    }
}

void runServer(const std::string& socket_port, const std::string& socket_ip){
    
    int sock = socket_setup(socket_port, &socket_ip);
    DPRINTF(DEBUG_METADATA_SERVER, "Alive port is %s\n", socket_port.c_str());
    
    while(1){
        int portid = stoi(socket_port);
        int new_sock = accept(sock, NULL, 0);
        std::thread cThread([new_sock, portid](){ server_connection(new_sock, portid); });
        cThread.detach();
    }
    
    close(sock);
}

int main(int argc, char** argv){

    signal(SIGPIPE, SIG_IGN);

//    std::cout << "AAAA" << std::endl;
    if(argc != 3){
        std::cout << "Enter the correct number of arguments: " << argv[0] << " <ext_ip> <port_no>" << std::endl;
        return -1;
    }
    
    runServer(argv[2], argv[1]);
    
    return 0;
}

