/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   ABD_Client.cpp
 * Author: shahrooz
 * 
 * Created on January 4, 2020, 11:34 PM
 */

#include "ABD_Client.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <regex>


ABD_Client::ABD_Client(Properties &prop, uint32_t client_id) {
    this->current_class = "ABD";
    this->id = client_id;
    this->prop = prop;
}

ABD_Client::~ABD_Client() {
    DPRINTF(DEBUG_ABD_Client, "client with id \"%u\" has been destructed.\n", this->id);
}

void _get_timestamp_ABD(std::string *key, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter, Server *server,
                    std::string current_class, std::vector<Timestamp*> *tss){  
    DPRINTF(DEBUG_ABD_Client, "started.\n");
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
    data.push_back("get_timestamp");
    data.push_back(*key);
    data.push_back(current_class);
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
    
    printf("get timestamp, data received is %s\n", data[1].c_str());
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

Timestamp* ABD_Client::get_timestamp_ABD(std::string *key, Placement &p){
    
    DPRINTF(DEBUG_ABD_Client, "started.\n");

    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    std::vector<Timestamp*> tss;
    Timestamp *ret = nullptr;
    
    for(std::vector<DC*>::iterator it = p.Q1.begin();
            it != p.Q1.end(); it++){
        std::thread th(_get_timestamp_ABD, key, &mtx, &cv, &counter,
                (*it)->servers[0], this->current_class, &tss);
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
    
    DPRINTF(DEBUG_ABD_Client, "finished successfully.\n");

    return ret;
}

void _put_ABD(std::string *key, std::string *value, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class){
    
    DPRINTF(DEBUG_ABD_Client, "started.\n");
    
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
    data.push_back("put");
    data.push_back(*key);
    data.push_back(*value);
    data.push_back(timestamp->get_string());
    data.push_back(current_class);
    
    std::cout<< "AAAAA: Sending Value  _PUT"<<(*value).size() << "value is "<< (*value) <<std::endl;
    if((*value).empty()){
	printf("WARNING!!! SENDING EMPTY STRING \n");
    }
    DataTransfer().sendMsg(sock, data);
    
    data.clear();
    DataTransfer().recvMsg(sock, data);
    
    std::unique_lock<std::mutex> lock(*mutex);
    (*counter)++;
    cv->notify_one();
    
    DPRINTF(DEBUG_ABD_Client, "finished successfully. with port: %uh\n", server->port);
    
    return;
}

uint32_t ABD_Client::put(std::string key, std::string value, Placement &p, bool insert){
    
//    std::vector <std::string*> chunks;
    
    Timestamp *timestamp = nullptr;
    Timestamp *tmp = nullptr;
    
    if(insert){ // This is a new key
        timestamp = new Timestamp(this->id);
    }
    else{
        tmp = this->get_timestamp_ABD(&key, p);
        timestamp = new Timestamp(tmp->increase_timestamp(this->id));
        delete tmp;
    }
    
//    for(int i = 0; i < p.Q2.size(); i++){
//        chunks.push_back(new std::string(value));
//    }
    
    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    
    int i = 0;
    
    for(std::vector<DC*>::iterator it = p.Q2.begin();
            it != p.Q2.end(); it++){
	printf("The port is: %uh", (*it)->servers[0]->port);
        std::thread th(_put_ABD, &key, &value, &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class);
        th.detach();
        i++;
    }
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q2.size()){
        cv.wait(lock);
    }
    lock.unlock();
    
    DPRINTF(DEBUG_ABD_Client, "finished successfully.\n");
    return S_OK;
}

void _get_ABD(std::string *key, std::vector<std::string*> *chunks, std::vector<Timestamp*> *tss,
                    std::mutex *mutex, std::condition_variable *cv, uint32_t *counter,
                    Server *server, std::string current_class){
    DPRINTF(DEBUG_ABD_Client, "started.\n");
    
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
    data.push_back("get");
    data.push_back(*key);
    data.push_back("None"); //ToDo: Need this??
    data.push_back(current_class);
    DataTransfer().sendMsg(sock, data);
    
    data.clear();
    DataTransfer().recvMsg(sock, data);
    std::string data_portion = data[1];
    
    std::string timestamp_str = data[2]; // tmp.substr(start_index, end_index - start_index);
    std::size_t dash_pos = timestamp_str.find("-");
    
    // make client_id and time regarding the received message
    uint32_t client_id_ts = stoi(timestamp_str.substr(0, dash_pos));
    uint32_t time_ts = stoi(timestamp_str.substr(dash_pos + 1));
    
    Timestamp* t = new Timestamp(client_id_ts, time_ts);
    
    std::unique_lock<std::mutex> lock(*mutex);
    chunks->push_back(new std::string(data_portion));
    tss->push_back(t);
    (*counter)++;
    cv->notify_one();
    
    DPRINTF(DEBUG_ABD_Client, "finished successfully.\n");
    
    return;
}

uint32_t ABD_Client::get(std::string key, std::string &value, Placement &p){
    
    value.clear();
   
    std::vector <std::string*> chunks;
    std::vector<Timestamp*> tss;
    
    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    int i = 0;
    
    for(std::vector<DC*>::iterator it = p.Q1.begin();
            it != p.Q1.end(); it++){
        std::thread th(_get_ABD, &key, &chunks, &tss, &mtx, &cv, &counter,
                (*it)->servers[0], this->current_class);
        th.detach();
        i++;
    }
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q1.size()){
        cv.wait(lock);
    }
    lock.unlock();
    
    
    //ToDo: make it faster
    Timestamp *max_timestamp = new Timestamp(Timestamp::max_timestamp(tss));
    for(i = 0; i < tss.size(); i++){
        if(max_timestamp->get_string() == tss[i]->get_string())
            break;
    }
    
    value = *chunks[i];
    
    // ToDo: check if all the timestamps are the same so you don't have to do a write
    
    for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
        delete *it;
    }
    
    
    for(std::vector<std::string*>::iterator it = chunks.begin(); it != chunks.end(); it++){
        delete *it;
    }
    
    //SHAHROOZ DO _PUT
    uint32_t counter2 = 0;
    std::mutex mtx2;
    std::condition_variable cv2;
    
    i = 0;
    
    for(std::vector<DC*>::iterator it = p.Q2.begin();
            it != p.Q2.end(); it++){
	printf("The port is: %uh", (*it)->servers[0]->port);
        std::thread th(_put_ABD, &key, &value, &mtx2, &cv2, &counter2,
                (*it)->servers[0], max_timestamp, this->current_class);
        th.detach();
        i++;
    }
    
    std::unique_lock<std::mutex> lock2(mtx2);
    while(counter2 < p.Q2.size()){
        cv2.wait(lock2);
    }
    lock2.unlock();
    
    
    delete max_timestamp;
    
    DPRINTF(DEBUG_ABD_Client, "Received value is: %s\n", value.c_str());
    
    return S_OK;
}


