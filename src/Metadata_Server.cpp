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

using namespace std;

/*
 * 
 */

std::map<std::string, Placement> ps;
std::mutex lock_t;

inline std::string construct_key_metadata(const std::string &key, const std::string &conf_id){
    std::string ret;
    ret += key;
    ret += "!";
    ret += conf_id;
    return ret;
}

void server_connection(int connection, int portid){

    std::string recvd;
    int result = DataTransfer::recvMsg(connection,recvd);
    if(result != 1){
        strVec msg{"failure", "BAD FORMAT MESSAGE RECEIVED"};
        DataTransfer::sendMsg(connection, DataTransfer::serialize(msg));
        close(connection);
        return;
    }

    // if data.size > 3
    // Data[0] -> method_name
    // Data[1] -> key
    // Data[2] -> timestamp
    strVec data = DataTransfer::deserialize(recvd);
    std::string &method = data[0]; // Method: ask/update, key, conf_id
    
    
    std::unique_lock<std::mutex> lock(lock_t);
    if(method == "ask"){
        DPRINTF(DEBUG_METADATA_SERVER, "The method ask is called. The key is %s, conf_id: %s, server port is %u\n",
                    data[1].c_str(), data[2].c_str(), portid);
        result = DataTransfer::sendMsg(connection, DataTransfer::serializePlacement(ps[construct_key_metadata(data[1], data[2])]));
    }else if(method == "update"){
        DPRINTF(DEBUG_METADATA_SERVER, "The method get is called. The key is %s, ts: %s, class: %s, server port is %u\n",
                    data[1].c_str(), data[2].c_str(), data[3].c_str(), portid);
        //std::cout << "GET fucntion called for server id "<< portid << std::endl;
        
        ps[construct_key_metadata(data[1], data[2])] = Placement(data[3]);
        result = DataTransfer::sendMsg(connection, DataTransfer::serialize({"OK"}));
    }
    else {
            DataTransfer::sendMsg(connection,  DataTransfer::serialize({"MethodNotFound", "Unknown method is called"}));
    }

    if(result != 1){
            DataTransfer::sendMsg(connection,  DataTransfer::serialize({"Failure","Server Response failed"}));
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

