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

#include <sys/time.h>

char fmt_abd[64];
char buf_abd[64];
struct timeval tv_abd;
struct tm *tm22_abd;

ABD_Client::ABD_Client(Properties &prop, uint32_t client_id) {
    this->current_class = "ABD";
    this->id = client_id;
    this->prop = prop;
    this->operation_id = 0;
    char name[20];
    name[0] = '0' + client_id;
    name[1] = '.';
    name[2] = 't';
    name[3] = 'x';
    name[4] = 't';
    name[5] = '\0';
    this->log_file = fopen(name, "w");
}

ABD_Client::~ABD_Client() {
    fclose(this->log_file);
    DPRINTF(DEBUG_ABD_Client, "client with id \"%u\" has been destructed.\n", this->id);
}

uint32_t ABD_Client::get_operation_id(){
    return operation_id;
}

void _get_timestamp_ABD(std::string *key, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter, Server *server,
                    std::string current_class, std::vector<Timestamp*> *tss,
                    int operation_id, ABD_Client *doer){  
    DPRINTF(DEBUG_ABD_Client, "started.\n");
    std::string key2 = *key;
    int sock = 0; 
    struct sockaddr_in serv_addr;
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){ 
        printf("\n Socket creation error \n"); 
        return; 
    }
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(server->port);
    std::string ip_str = server->ip;
    
    if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){ 
        printf("\nInvalid address/ Address not supported \n"); 
        return; 
    } 
   
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
    strVec data;
    data.push_back("get_timestamp");
    data.push_back(key2);
    data.push_back(current_class);
    DataTransfer().sendMsg(sock, DataTransfer::serialize(data));

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
    
//    printf("get timestamp, data received is %s\n", data[1].c_str());
    std::string timestamp_str = data[1]; // tmp.substr(start_index, end_index - start_index);
    
    std::size_t dash_pos = timestamp_str.find("-");
    
    // make client_id and time regarding the received message
    uint32_t client_id_ts = stoi(timestamp_str.substr(0, dash_pos));
    uint32_t time_ts = stoi(timestamp_str.substr(dash_pos + 1));
    
    Timestamp* t = new Timestamp(client_id_ts, time_ts);
    
    
    if(doer->get_operation_id() == operation_id){
        std::unique_lock<std::mutex> lock(*mutex);
        tss->push_back(t);
        (*counter)++;
        cv->notify_one();
    }
    else{
        delete t;
    }
    
    close(sock);

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
                (*it)->servers[0], this->current_class, &tss, this->operation_id, this);
        th.detach();
    }
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q1.size()/ 2 + 1){
        cv.wait(lock);
    }
    
    ret = new Timestamp(Timestamp::max_timestamp(tss));
    
    for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
        delete *it;
    }
    
    lock.unlock();
    
//    printf("Max timestamp found: %s\n", ret->get_string().c_str());
    
    this->operation_id++;
    
    DPRINTF(DEBUG_ABD_Client, "finished successfully.\n");

    return ret;
}

void _put_ABD(std::string *key, std::string *value, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class, int operation_id, ABD_Client *doer){
    
    DPRINTF(DEBUG_ABD_Client, "started.\n");
    
    std::string key2 = *key;
    std::string value2 = *value;
    Timestamp timestamp2 = *timestamp;
    
    int sock = 0; 
    struct sockaddr_in serv_addr;
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){ 
        printf("\n Socket creation error \n"); 
        return; 
    }
    
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(server->port);
    std::string ip_str = server->ip;
    
    if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){ 
        printf("\nInvalid address/ Address not supported \n"); 
        return; 
    } 
   
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
    strVec data;
    data.push_back("put");
    data.push_back(key2);
    data.push_back(value2);
    data.push_back(timestamp2.get_string());
    data.push_back(current_class);
    
//    std::cout<< "AAAAA: Sending Value  _PUT"<<(*value).size() << "value is "<< (*value) <<std::endl;
    if((value2).empty()){
	printf("WARNING!!! SENDING EMPTY STRING \n");
    }
    DataTransfer().sendMsg(sock, DataTransfer::serialize(data));
    
    data.clear();
    std::string recvd;
    DataTransfer::recvMsg(sock, recvd); // data must have the timestamp.
    data =  DataTransfer::deserialize(recvd);
    
    // Todo: parse data
    
    
    if(doer->get_operation_id() == operation_id){
        std::unique_lock<std::mutex> lock(*mutex);
        (*counter)++;
        cv->notify_one();
//        printf("AAA\n");
    }
    
    DPRINTF(DEBUG_ABD_Client, "finished successfully. with port: %u\n", server->port);
    close(sock);
    
    return;
}

uint32_t ABD_Client::put(std::string key, std::string value, Placement &p, bool insert){
    
    printf("BBBBB\n");
    
//    std::vector <std::string*> chunks;
    
    gettimeofday (&tv_abd, NULL);
    tm22_abd = localtime (&tv_abd.tv_sec);
    strftime (fmt_abd, sizeof (fmt_abd), "%H:%M:%S:%%06u", tm22_abd);
    snprintf (buf_abd, sizeof (buf_abd), fmt_abd, tv_abd.tv_usec);
    fprintf(this->log_file, "%s write invoke %s\n", buf_abd, value.c_str());
    
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
//	printf("The port is: %uh", (*it)->servers[0]->port);
        std::thread th(_put_ABD, &key, &value, &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class, this->operation_id, this);
        th.detach();
        i++;
    }
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q2.size() / 2 + 1){
        cv.wait(lock);
    }
    
    this->operation_id++;
    
    DPRINTF(DEBUG_ABD_Client, "finished successfully.\n");
    
    gettimeofday (&tv_abd, NULL);
    tm22_abd = localtime (&tv_abd.tv_sec);
    strftime (fmt_abd, sizeof (fmt_abd), "%H:%M:%S:%%06u", tm22_abd);
    snprintf (buf_abd, sizeof (buf_abd), fmt_abd, tv_abd.tv_usec);
    fprintf(this->log_file, "%s write ok %s\n", buf_abd, value.c_str());
    
    return S_OK;
}

void _get_ABD(std::string *key, std::vector<std::string*> *chunks, std::vector<Timestamp*> *tss,
                    std::mutex *mutex, std::condition_variable *cv, uint32_t *counter,
                    Server *server, std::string current_class, int operation_id, ABD_Client *doer){
    
    DPRINTF(DEBUG_ABD_Client, "started.\n");
    std::string key2 = *key;
    
    int sock = 0;
    struct sockaddr_in serv_addr;
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){ 
        printf("\n Socket creation error \n"); 
        return; 
    }
    
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(server->port);
    std::string ip_str = server->ip;
    
    if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){ 
        printf("\nInvalid address/ Address not supported \n"); 
        return; 
    }
    
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
    strVec data;
    data.push_back("get");
    data.push_back(key2);
    data.push_back("None"); //ToDo: Need this??
    data.push_back(current_class);
    DataTransfer().sendMsg(sock, DataTransfer::serialize(data));
    
    data.clear();
    std::string recvd;
    DataTransfer::recvMsg(sock, recvd); // data must have the timestamp.
    data =  DataTransfer::deserialize(recvd);
    std::string data_portion = data[1];
    
    std::string timestamp_str = data[2]; // tmp.substr(start_index, end_index - start_index);
    std::size_t dash_pos = timestamp_str.find("-");
    
    // make client_id and time regarding the received message
    uint32_t client_id_ts = stoi(timestamp_str.substr(0, dash_pos));
    uint32_t time_ts = stoi(timestamp_str.substr(dash_pos + 1));
    
    Timestamp* t = new Timestamp(client_id_ts, time_ts);
    
    
    if(doer->get_operation_id() == operation_id){
        std::unique_lock<std::mutex> lock(*mutex);
        chunks->push_back(new std::string(data_portion));
        tss->push_back(t);
        (*counter)++;
        cv->notify_one();
    }
    
    DPRINTF(DEBUG_ABD_Client, "finished successfully.\n");
    close(sock);
    
    return;
}

uint32_t ABD_Client::get(std::string key, std::string &value, Placement &p){
    
    printf("AAAAA\n");
    
    gettimeofday (&tv_abd, NULL);
    tm22_abd = localtime (&tv_abd.tv_sec);
    strftime (fmt_abd, sizeof (fmt_abd), "%H:%M:%S:%%06u", tm22_abd);
    snprintf (buf_abd, sizeof (buf_abd), fmt_abd, tv_abd.tv_usec);
    fprintf(this->log_file, "%s read invoke nil\n", buf_abd);
    
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
                (*it)->servers[0], this->current_class, this->operation_id, this);
        th.detach();
        i++;
    }
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q1.size()){
        cv.wait(lock);
    }
    lock.unlock();
    
    this->operation_id++;
    
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
//	printf("The port is: %uh", (*it)->servers[0]->port);
        std::thread th(_put_ABD, &key, &value, &mtx2, &cv2, &counter2,
                (*it)->servers[0], max_timestamp, this->current_class, this->operation_id, this);
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
    
    gettimeofday (&tv_abd, NULL);
    tm22_abd = localtime (&tv_abd.tv_sec);
    strftime (fmt_abd, sizeof (fmt_abd), "%H:%M:%S:%%06u", tm22_abd);
    snprintf (buf_abd, sizeof (buf_abd), fmt_abd, tv_abd.tv_usec);
    fprintf(this->log_file, "%s read ok %s\n", buf_abd, value.c_str());
    
    
    return S_OK;
}

