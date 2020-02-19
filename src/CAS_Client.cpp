/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   CAS_Client.cpp
 * Author: shahrooz
 * 
 * Created on January 4, 2020, 11:35 PM
 */

#include "CAS_Client.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <regex>

//ToDo: what is data_center???? Do we need this thing?
CAS_Client::CAS_Client(Properties &prop, uint32_t local_datacenter_id,
        uint32_t data_center, uint32_t client_id) {
    
    timeout_per_request = 0; //ToDo: must be set by properties         self.timeout_per_request = int(properties["timeout_per_request"])

    ec_type = "liberasurecode_rs_vand";
    uint32_t timeout = 0; //ToDo: must be set by properties         self.timeout_per_request = int(properties["timeout_per_request"])
    this->data_center = data_center;
    this->id = client_id;
    this->local_datacenter_id = local_datacenter_id;
    current_class = "CAS";
    encoding_byte = "latin-1";
}

CAS_Client::~CAS_Client() {
}

int send_msg(int sock, const std::string &data){
    
    std::string data_copy = data;
    
    // add four byte length in network order in the beginning of the message
    uint32_t size = data_copy.size();
    size = htonl(size);
    uint8_t size_c[4];
    size_c[0] = ((uint8_t*)(&size))[0];
    size_c[1] = ((uint8_t*)(&size))[1];
    size_c[2] = ((uint8_t*)(&size))[2];
    size_c[3] = ((uint8_t*)(&size))[3];
    data_copy.insert(0, (char*)size_c, 4);
    
    return send(sock , data_copy.c_str() , data_copy.size() , 0);
}

// ToDo: you may need to implement the socket in non-blocking mode!
void _get_timestamp(uint32_t key, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter, Server *server,
                    std::string current_class, std::vector<Timestamp*> *tss){    
    int sock = 0; 
    struct sockaddr_in serv_addr;
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){ 
        printf("\n Socket creation error \n"); 
        return; 
    }
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(server->port);
    in_addr ip_in_addr;
    ip_in_addr.s_addr = server->ip;
    
    if(inet_pton(AF_INET, inet_ntoa(ip_in_addr), &serv_addr.sin_addr) <= 0){ 
        printf("\nInvalid address/ Address not supported \n"); 
        return; 
    } 
   
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
    std::string data =
        "{\n"\
            "\"\tmethod\": \"get_timestamp\"\n"\
            "\"\tkey\": \""+ std::to_string(key) + "\"\n"\
            "\tclass: \"" + current_class + "\"\n"\
        "}";
    
    send_msg(sock, data);
    
//    sock.settimeout(self.timeout_per_request)

    uint8_t buf[640000];
    read(sock, buf, 640000);
//    sock.close();
    
    // make client_id and time regarding the received message
    uint32_t client_id_ts;
    uint32_t time_ts;
    
    Timestamp* t = new Timestamp(client_id_ts, time_ts);
    
    std::unique_lock<std::mutex> lock(*mutex);
    tss->push_back(t);
    (*counter)++;
    cv->notify_one();

    return;
}

Timestamp* CAS_Client::get_timestamp(uint32_t key){
    
    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    std::vector<std::thread*> threads;
    std::vector<Timestamp*> tss;
    Timestamp *ret = nullptr;
    
    for(std::vector<DC*>::iterator it = this->prop.datacenters.begin();
            it != this->prop.datacenters.end(); it++){
        std::thread th(_get_timestamp, key, &mtx, &cv, &counter,
                (*it)->servers[0], this->current_class, &tss);
        threads.push_back(&th);
    }
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < prop.datacenters.size()){
        cv.wait(lock);
    }
    lock.unlock();
    
    ret = new Timestamp(Timestamp::max_timestamp(tss));
    for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
        delete *it;
    }
    
    return ret;
}

void _put(uint32_t key, std::string *value, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class){
    
    int sock = 0; 
    struct sockaddr_in serv_addr;
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){ 
        printf("\n Socket creation error \n"); 
        return; 
    }
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(server->port);
    in_addr ip_in_addr;
    ip_in_addr.s_addr = server->ip;
    
    if(inet_pton(AF_INET, inet_ntoa(ip_in_addr), &serv_addr.sin_addr) <= 0){ 
        printf("\nInvalid address/ Address not supported \n"); 
        return; 
    } 
   
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
    std::string data =
        "{\n"\
            "\t\"method\": put\n"\
            "\t\"key\": \""+ std::to_string(key) + "\"\n"\
            "\t\"value\": \""+ (*value) + "\"\n"\
            "\t\"timestamp\": \""+ timestamp->get_string() + "\"\n"\
            "\t\"class\": \"" + current_class + "\"\n"\
        "}";
    
    send_msg(sock, data);
    
//    sock.settimeout(self.timeout_per_request)

    uint8_t buf[640000];
    read(sock, buf, 640000);
//    sock.close();
    
    
    std::unique_lock<std::mutex> lock(*mutex);
    (*counter)++;
    cv->notify_one();

    return;
    
}

void _put_fin(uint32_t key, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class){
    
    int sock = 0; 
    struct sockaddr_in serv_addr;
    if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){ 
        printf("\n Socket creation error \n"); 
        return; 
    }
    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(server->port);
    in_addr ip_in_addr;
    ip_in_addr.s_addr = server->ip;
    
    if(inet_pton(AF_INET, inet_ntoa(ip_in_addr), &serv_addr.sin_addr) <= 0){ 
        printf("\nInvalid address/ Address not supported \n"); 
        return; 
    } 
   
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
    std::string data =
        "{\n"\
            "\t\"method\": put_fin\n"\
            "\t\"key\": \""+ std::to_string(key) + "\"\n"\
            "\t\"timestamp\": \""+ timestamp->get_string() + "\"\n"\
            "\t\"class\": \"" + current_class + "\"\n"\
        "}";
    
    send_msg(sock, data);
    
//    sock.settimeout(self.timeout_per_request)

    uint8_t buf[640000];
    read(sock, buf, 640000);
//    sock.close();
    
    
    std::unique_lock<std::mutex> lock(*mutex);
    (*counter)++;
    cv->notify_one();

    return;
    
}

void encode(const std::string * const data, std::vector <std::string*> *chunks,
                        struct ec_args * const args){
    
    int rc = 0;
    int desc = -1;
    int orig_data_size = 1024 * 1024;
    char *orig_data = NULL;
    char **encoded_data = NULL, **encoded_parity = NULL;
    uint64_t encoded_fragment_len = 0;

    desc = liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, args);

    if (-EBACKENDNOTAVAIL == desc) {
        fprintf(stderr, "Backend library not available!\n");
        return;
    } else if ((args->k + args->m) > EC_MAX_FRAGMENTS) {
        assert(-EINVALIDPARAMS == desc);
        return;
    } else
        assert(desc > 0);

//    orig_data = create_buffer(orig_data_size, 'x');
    orig_data = (char*) data->c_str();
    assert(orig_data != NULL);
    rc = liberasurecode_encode(desc, orig_data, data->size(),
            &encoded_data, &encoded_parity, &encoded_fragment_len);
    assert(0 == rc); // ToDo: Add a lot of crash handler...
    
    for (int i = 0; i < args->k + args->m; i++)
    {
        int cmp_size = -1;
        char *data_ptr = NULL;
        char *frag = NULL;

        frag = (i < args->k) ? encoded_data[i] : encoded_parity[i - args->k];
        assert(frag != NULL);
        
        chunks->push_back(new std::string(frag, encoded_fragment_len));
        
    }
    
    rc = liberasurecode_encode_cleanup(desc, encoded_data, encoded_parity);
    assert(rc == 0);

    assert(0 == liberasurecode_instance_destroy(desc));
    
    return;
}

uint32_t CAS_Client::put(uint32_t key, std::string value, Placement &p, bool insert){
    
    std::vector <std::string*> chunks;
    struct ec_args null_args;
    null_args.k = p.k;
    null_args.m = p.m - p.k;
    null_args.w = 16; // ToDo: what must it be?
    null_args.ct = CHKSUM_NONE;
    
    std::thread encoder(encode, &value, &chunks, &null_args);
    
    Timestamp *timestamp = nullptr;
    
    
    if(insert){ // This is a new key
        timestamp = new Timestamp(this->id);
    }
    else{
        timestamp = this->get_timestamp(key);
    }
    
    // ToDo: join the encoder thread
    encoder.join();
    
    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    std::vector<std::thread*> threads;
    
    // prewrite
    int i = 0;
    for(std::vector<DC*>::iterator it = p.Q2.begin();
            it != p.Q2.end(); it++){
        std::thread th(_put, key, (chunks[i]), &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class);
        threads.push_back(&th);
        i++;
    }
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q2.size()){
        cv.wait(lock);
    }
    lock.unlock();
    
    
    // fin tag
    counter = 0;
    threads.clear(); // ToDo: Do we need this?
    for(std::vector<DC*>::iterator it = p.Q3.begin();
            it != p.Q3.end(); it++){
        std::thread th(_put_fin, key, &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class);
        threads.push_back(&th);
        i++;
    }
    
    for(int i = 0; i < chunks.size(); i++){
        delete chunks[i];
    }
    
    std::unique_lock<std::mutex> lock2(mtx);
    while(counter < p.Q3.size()){
        cv.wait(lock2);
    }
    lock2.unlock();
    
    return S_OK;
}

