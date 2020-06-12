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
#include <algorithm>

//Config::Config(Timestamp& timestamp, Group &group){
//    this->timestamp = new Timestamp(timestamp);
//    this->group = new Group(group);
//}
//
//Config::~Config(){
//    delete timestamp;
//    delete group;
//}

Reconfig *Reconfig::instance = nullptr;

int Reconfig::reconfig(GroupConfig &old_config, GroupConfig &new_config){
    if(this->instance == nullptr){
        this->instance = new Reconfig();
        instance->id = 10000; // ToDo: We should set a common id for controller.
        
//        char name[30];
//        this->operation_id = 0;
//        name[0] = 'l';
//        name[1] = 'o';
//        name[2] = 'g';
//        name[3] = 's';
//        name[4] = '/';
//
//        sprintf(&name[5], "cont.reconfig.txt");
//        this->log_file = fopen(name, "w");
    }
    
    /// ToDo: implement the Vanila reconfiguration
    
    
    
    return 0;
}

static Reconfig* Reconfig::get_instance(){
    return instance;
}

uint32_t Reconfig::get_operation_id(){
    return operation_id;
}

void _send_reconfig_query(std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter, Server *server,
                    std::vector<Timestamp*> *tss, std::vector<string*> *vs, bool is_cas,
                    uint32_t operation_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");
//    std::string key2 = *key;
    int sock = 0;
    struct sockaddr_in serv_addr;
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        printf("\n Socket creation error \n");
        return;
    }
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server->port);
    std::string ip_str = server->ip;
    //std::string ip_str = convert_ip_to_string(server->ip);

    if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){
        printf("\nInvalid address/ Address not supported \n");
        return;
    }

    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
        printf("\nConnection Failed \n");
        return;
    }

    strVec data;
    data.push_back("reconfig_query");
    DataTransfer::sendMsg(sock, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;
    DataTransfer::recvMsg(sock, recvd); // data must have the timestamp.
    data =  DataTransfer::deserialize(recvd);

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


    if(data[0] == "OK"){
        if(is_cas){
            printf("_send_reconfig_query, data received is %s and %s\n", data[1].c_str(), data[2].c_str());
            std::string timestamp_str = data[1];

            std::size_t dash_pos = timestamp_str.find("-");
            if(dash_pos >= timestamp_str.size()){
                std::cerr << "SUbstr violated !!!!!!!" << timestamp_str <<std::endl;
            }

            // make client_id and time regarding the received message
            uint32_t client_id_ts = stoi(timestamp_str.substr(0, dash_pos));
            uint32_t time_ts = stoi(timestamp_str.substr(dash_pos + 1));

            Timestamp* t = new Timestamp(client_id_ts, time_ts);

            std::unique_lock<std::mutex> lock(*mutex);
            if(Reconfig::get_instance()->get_operation_id() == operation_id){
                vs->push_back(new string(data[2]));
                tss->push_back(t);
                (*counter)++;
                cv->notify_one();
            }
            else{
                delete t;
            }
        }
        else{ // this is for ABD protocol
            printf("_send_reconfig_query, data received is %s\n", data[1].c_str());
            std::string timestamp_str = data[1];

            std::size_t dash_pos = timestamp_str.find("-");
            if(dash_pos >= timestamp_str.size()){
                std::cerr << "SUbstr violated !!!!!!!" << timestamp_str <<std::endl;
            }

            // make client_id and time regarding the received message
            uint32_t client_id_ts = stoi(timestamp_str.substr(0, dash_pos));
            uint32_t time_ts = stoi(timestamp_str.substr(dash_pos + 1));

            Timestamp* t = new Timestamp(client_id_ts, time_ts);

            std::unique_lock<std::mutex> lock(*mutex);
            if(Reconfig::get_instance()->get_operation_id() == operation_id){
                tss->push_back(t);
                (*counter)++;
                cv->notify_one();
            }
            else{
                delete t;
            }
        }
    }
    else{
        std::unique_lock<std::mutex> lock(*mutex);
    }

    close(sock);
    return;
    
    /////
    
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
//    std::string timestamp_str = data[1]; // tmp.substr(start_index, end_index - start_index);
//    
//    std::size_t dash_pos = timestamp_str.find("-");
//    
//    // make client_id and time regarding the received message
//    uint32_t client_id_ts = stoi(timestamp_str.substr(0, dash_pos));
//    uint32_t time_ts = stoi(timestamp_str.substr(dash_pos + 1));
//    
//    Timestamp* t = new Timestamp(client_id_ts, time_ts);
//    
//    std::unique_lock<std::mutex> lock(*mutex);
//    tss->push_back(t);
//    (*counter)++;
//    cv->notify_one();

    return;
}

int Reconfig::send_reconfig_query(GroupConfig &old_config, Timestamp* ret_ts, string *ret_v)){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");

    
    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    std::vector<Timestamp*> tss;
    std::vector<string*> vs;
//    Timestamp *ret = nullptr;
    
    std::vector<DC*> N;
    Placement &p = *(old_config.placement_p);
    for(std::vector<DC*>::iterator it = p.Q1.begin();
            it != p.Q1.end(); it++){
        if(std::find(N.begin(), N.end(), (*it)) == N.end()){
            N.push_back((*it));
        }
    }
    for(std::vector<DC*>::iterator it = p.Q2.begin();
            it != p.Q2.end(); it++){
        if(std::find(N.begin(), N.end(), (*it)) == N.end()){
            N.push_back((*it));
        }
    }
    for(std::vector<DC*>::iterator it = p.Q3.begin();
            it != p.Q3.end(); it++){
        if(std::find(N.begin(), N.end(), (*it)) == N.end()){
            N.push_back((*it));
        }
    }
    for(std::vector<DC*>::iterator it = p.Q4.begin();
            it != p.Q4.end(); it++){
        if(std::find(N.begin(), N.end(), (*it)) == N.end()){
            N.push_back((*it));
        }
    }
    
    if(old_config.placement_p->protocol == "ABD"){
        for(std::vector<DC*>::iterator it = N.begin();
                it != N.end(); it++){ // ToDo: we have to change it to N and then wait for max(N − f , N − Q2 + 1)
            std::thread th(_send_reconfig_query, &mtx, &cv, &counter,
                    (*it)->servers[0], &tss, &vs, false, instance->operation_id);
            th.detach();
        }
        
//        for(std::vector<DC*>::iterator it = p.Q2.begin();
//                it != p.Q2.end(); it++){
//            std::thread th(_send_reconfig_query, &mtx, &cv, &counter,
//                    (*it)->servers[0], &tss, vs, false, instance->operation_id);
//            th.detach();
//        }

        std::unique_lock<std::mutex> lock(mtx);
        int f = 0; // TODO: we need to obtain this number from the placement.
        while(counter < std::max(N.size() - p.f, N.size() - p.Q2.size() + 1){
            cv.wait(lock);
        }
        
        int idd;
        int ii = 0;
        std::vector<Timestamp*>::iterator it_max = tss.begin();
	for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
            if((*it)->time > (*it_max)->time ||
                    ((*it)->time == (*it_max)->time && (*it)->client_id < (*it_max)->client_id)){
                    it_max = it;
                    idd == ii;
            }
            ii++;
	}
        
        
        ret_ts = new Timestamp(*tss[idd]);
        for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
            delete *it;
        }
        
        ret_v = new string(*vs[idd]);
        for(std::vector<Timestamp*>::iterator it = vs.begin(); it != vs.end(); it++){
            delete *it;
        }
        
        instance->operation_id++;
        lock.unlock();
    }
    else{ // CAS
        
        for(std::vector<DC*>::iterator it = N.begin();
                it != N.end(); it++){
            std::thread th(_send_reconfig_query, &mtx, &cv, &counter,
                    (*it)->servers[0], &tss, vs, true, instance->operation_id);
            th.detach();
        }

        std::unique_lock<std::mutex> lock(mtx);
        while(counter < std::max(N.size() - p.f, N.size() - p.Q3.size() + 1, N.size() - p.Q4.size() + 1)){
            cv.wait(lock);
        }
        
        ret_ts = new Timestamp(Timestamp::max_timestamp(tss));
        for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
            delete *it;
        }
        
        ret_v = nullptr;
        
        instance->operation_id++;
        lock.unlock();
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

