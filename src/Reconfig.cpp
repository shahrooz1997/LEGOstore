/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Reconfig.cpp
 * Author: shahrooz
 * 
 * Created on March 23, 2020, 6:34 PM
 */

#include "Reconfig.h"

Config::Config(Timestamp& timestamp, Group &group){
    this->timestamp = new Timestamp(timestamp);
    this->group = new Group(group);
}

Config::~Config(){
    delete timestamp;
    delete group;
}

Reconfig *Reconfig::instance = nullptr;

int Reconfig::reconfig(Config &old_config, Config &new_config){
    if(this->instance == nullptr)
        this->instance = new Reconfig();
    
    /// ToDo: implement the Vanila reconfiguration
    
    return 0;
}

void _send_reconfig_query(std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter, Server *server,
                    std::vector<Timestamp*> *tss){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");
    int sock = 0; 
    struct sockaddr_in serv_addr;
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){ 
        printf("\n Socket creation error \n"); 
        return; 
    }
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(server->port);
    std::string ip_str = convert_ip_to_string(server->ip);
    
    if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){ 
        printf("\nInvalid address/ Address not supported \n"); 
        return; 
    } 
   
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
    valueVec data;
    data.push_back("reconfig_query");
    DataTransfer().sendMsg(sock, data);

    data.clear();
    DataTransfer().recvMsg(sock, data); // data must have the timestamp.

    
//    std::string tmp((const char*)buf, buf_size);
//    
//    std::size_t pos = tmp.find("timestamp");
//    pos = tmp.find(":", pos);
//    std::size_t start_index = tmp.find("\"", pos);
//    if(start_index == std::string::npos){
//        return;
//    }
//    start_index++;
//    std::size_t end_index = tmp.find("\"", start_index);
    
//	printf("get timestamp, data received is %s\n", data[1].c_str());
    std::string timestamp_str = data[1]; // tmp.substr(start_index, end_index - start_index);
    
    std::size_t dash_pos = timestamp_str.find("-");
    
    // make client_id and time regarding the received message
    uint32_t client_id_ts = stoi(timestamp_str.substr(0, dash_pos));
    uint32_t time_ts = stoi(timestamp_str.substr(dash_pos + 1));
    
    Timestamp* t = new Timestamp(client_id_ts, time_ts);
    
    std::unique_lock<std::mutex> lock(*mutex);
    tss->push_back(t);
    (*counter)++;
    cv->notify_one();

    return;
}

int Reconfig::send_reconfig_query(Config &old_config){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");

    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    std::vector<Timestamp*> tss;
    Timestamp *ret = nullptr;
    
    if(old_config.group->placement_p->protocol == "ABD"){
        Placement &p = *(old_config.group->placement_p);
        for(std::vector<DC*>::iterator it = p.Q1.begin();
                it != p.Q1.end(); it++){
            std::thread th(_send_reconfig_query, &mtx, &cv, &counter,
                    (*it)->servers[0], &tss);
            th.detach();
        }
        
        for(std::vector<DC*>::iterator it = p.Q2.begin();
                it != p.Q2.end(); it++){
            std::thread th(_send_reconfig_query, &mtx, &cv, &counter,
                    (*it)->servers[0], &tss);
            th.detach();
        }

        std::unique_lock<std::mutex> lock(mtx);
        while(counter < p.Q1.size() + p.Q1.size()){
            cv.wait(lock);
        }
        lock.unlock();

        ret = new Timestamp(Timestamp::max_timestamp(tss));
        for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
            delete *it;
        }
    }
    else{
        for(std::vector<DC*>::iterator it = p.Q1.begin();
                it != p.Q1.end(); it++){
            std::thread th(_send_reconfig_query, &mtx, &cv, &counter,
                    (*it)->servers[0], &tss);
            th.detach();
        }

        std::unique_lock<std::mutex> lock(mtx);
        while(counter < p.Q1.size()){
            cv.wait(lock);
        }
        lock.unlock();

        ret = new Timestamp(Timestamp::max_timestamp(tss));
        for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
            delete *it;
        }
    }
    
    for(std::vector<DC*>::iterator it = p.Q1.begin();
            it != p.Q1.end(); it++){
        std::thread th(_send_reconfig_query, &mtx, &cv, &counter,
                (*it)->servers[0], &tss);
        th.detach();
    }
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q1.size()){
        cv.wait(lock);
    }
    lock.unlock();
    
    ret = new Timestamp(Timestamp::max_timestamp(tss));
    for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
        delete *it;
    }
    
    DPRINTF(DEBUG_RECONFIG_CONTROL, "finished successfully.\n");

    return ret;
}

Reconfig::Reconfig() {
}

Reconfig::~Reconfig() {
    delete this->instance;
    this->instance = nullptr;
}

