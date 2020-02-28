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
#include "ABD_Client.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <regex>


//TODO: there is a big design problem with prop... resolve it.

//ToDo: what is data_center???? Do we need this thing?
CAS_Client::CAS_Client(Properties &prop, uint32_t local_datacenter_id,
        uint32_t data_center, uint32_t client_id) {
    
    timeout_per_request = 0; //ToDo: must be set by properties         self.timeout_per_request = int(properties["timeout_per_request"])

    ec_type = "liberasurecode_rs_vand";
    uint32_t timeout = 0; //ToDo: must be set by properties         self.timeout_per_request = int(properties["timeout_per_request"])
    this->data_center = data_center;
    this->id = client_id;
    this->local_datacenter_id = local_datacenter_id;
    current_class = "Viveck_1"; //"CAS";
    encoding_byte = "latin-1";
    this->prop = prop;
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
void _get_timestamp(std::string *key, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter, Server *server,
                    std::string current_class, std::vector<Timestamp*> *tss){  
    DPRINTF(DEBUG_CAS_Client, "started.\n");
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
    
//    std::string data =
//        "{\n"\
//            "\"\tmethod\": \"get_timestamp\"\n"\
//            "\"\tkey\": \""+ (*key) + "\"\n"\
//            "\tclass: \"" + current_class + "\"\n"\
//        "}";
    
    std::string data = "get_timestamp";
    data += "+:--:+";
    data += *key;
    data += "+:--:+";
    data += current_class;
    
    send_msg(sock, data);
    
//    sock.settimeout(self.timeout_per_request)

    uint8_t buf[640000];
    int buf_size = read(sock, buf, 640000);
//    printf("shahrooz:\n");
//    for(int i = 0; i < buf_size; i++){
//        printf("%c", buf[i]);
//    }
//    printf("\n\n");
//    sock.close();
    
    std::string tmp((const char*)buf, buf_size);
    
    std::size_t pos = tmp.find("timestamp");
    pos = tmp.find(":", pos);
    std::size_t start_index = tmp.find("\"", pos);
    if(start_index == std::string::npos){
        return;
    }
    start_index++;
    std::size_t end_index = tmp.find("\"", start_index);
    
    std::string timestamp_str = tmp.substr(start_index, end_index - start_index);
    
    std::size_t dash_pos = timestamp_str.find("-");
    
//    DPRINTF(DEBUG_CAS_Client, "time: %s\n", timestamp_str.c_str());
    
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

Timestamp* CAS_Client::get_timestamp(std::string *key){
    
    DPRINTF(DEBUG_CAS_Client, "started.\n");

    
    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
//    std::vector<std::thread*> threads;
    std::vector<Timestamp*> tss;
    Timestamp *ret = nullptr;
    
    
    
    for(std::vector<DC*>::iterator it = this->prop.datacenters.begin();
            it != this->prop.datacenters.end(); it++){
        std::thread th(_get_timestamp, key, &mtx, &cv, &counter,
                (*it)->servers[0], this->current_class, &tss);
        th.detach();
//        threads.push_back(&th);
    }
//        printf("aaaaa %lu\n", counter);

    
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < prop.datacenters.size() - 2){
        cv.wait(lock);
    }
    lock.unlock();
    
//    printf("aaaaa %lu\n", counter);
    
    ret = new Timestamp(Timestamp::max_timestamp(tss));
    for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
        delete *it;
    }
    
    DPRINTF(DEBUG_CAS_Client, "finished successfully.\n");

    return ret;
}

void _put(std::string *key, std::string *value, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class){
    
    DPRINTF(DEBUG_CAS_Client, "started.\n");
    
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
   
//    printf("sutck here");
    //ToDo: take care of connect by putting a timeout over it
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
//    printf("I am connected\n");

    
//    std::string data =
//        "{\n"\
//            "\t\"method\": put\n"\
//            "\t\"key\": \""+ (*key) + "\"\n"\
//            "\t\"value\": \""+ (*value) + "\"\n"\
//            "\t\"timestamp\": \""+ timestamp->get_string() + "\"\n"\
//            "\t\"class\": \"" + current_class + "\"\n"\
//        "}";
    
    std::string data = "put+:--:+";
    data += *key;
    data += "+:--:+";
    data += *value;
    data += "+:--:+";
    data += timestamp->get_string();
    data += "+:--:+";
    data += "Viveck_1";
    
    
    // ToDo: test these functions
    send_msg(sock, data);
    
//    sock.settimeout(this->timeout_per_request);

    // ToDo: read the status of server
    uint8_t buf[640000];
    read(sock, buf, 640000);
//    sock.close();
    
    std::unique_lock<std::mutex> lock(*mutex);
    (*counter)++;
    cv->notify_one();
    
    DPRINTF(DEBUG_CAS_Client, "finished successfully.\n");
    
    return;
    
}

void _put_fin(std::string *key, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class){
    
    DPRINTF(DEBUG_CAS_Client, "started.\n");
    
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
   
    
    //ToDo:
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
//    std::string data =
//        "{\n"\
//            "\t\"method\": put_fin\n"\
//            "\t\"key\": \""+ (*key) + "\"\n"\
//            "\t\"timestamp\": \""+ timestamp->get_string() + "\"\n"\
//            "\t\"class\": \"" + current_class + "\"\n"\
//        "}";
    
    std::string data = "put_fin+:--:+";
    data += *key;
    data += "+:--:+";
    data += timestamp->get_string();
    data += "+:--:+";
    data += current_class;

    //ToDo:
    send_msg(sock, data);
    
//    sock.settimeout(self.timeout_per_request)

    // ToDo: read the status returned by the server
    uint8_t buf[640000];
    read(sock, buf, 640000);
//    sock.close();
    
    
    std::unique_lock<std::mutex> lock(*mutex);
    (*counter)++;
    cv->notify_one();
    
    DPRINTF(DEBUG_CAS_Client, "finished successfully.\n");
    
    return;
}

void encode(const std::string * const data, std::vector <std::string*> *chunks,
                        struct ec_args * const args){
    
    int rc = 0;
    int desc = -1;
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

int create_frags_array(char ***array,
                              char **data,
                              char **parity,
                              struct ec_args *args,
                              int *skips)
{
    // N.B. this function sets pointer reference to the ***array
    // from **data and **parity so DO NOT free each value of
    // the array independently because the data and parity will
    // be expected to be cleanup via liberasurecode_encode_cleanup
    int num_frags = 0;
    int i = 0;
    char **ptr = NULL;
    *array = (char **) malloc((args->k + args->m) * sizeof(char *));
    if (array == NULL) {
        num_frags = -1;
        goto out;
    }
    //add data frags
    ptr = *array;
    for (i = 0; i < args->k; i++) {
        if (data[i] == NULL || skips[i] == 1) {
            printf("%d skipped1\n", i);
            continue;
        }
        *ptr++ = data[i];
        num_frags++;
    }
    //add parity frags
    for (i = 0; i < args->m; i++) {
        if (parity[i] == NULL || skips[i + args->k] == 1) {
            printf("%d skipped2\n", i);
            continue;
        }
        *ptr++ = parity[i];
        num_frags++;
    }
out:
    return num_frags;
}

void decode(std::string *data, std::vector <std::string*> *chunks,
                        struct ec_args * const args){
    
    int i = 0;
    int rc = 0;
    int desc = -1;
    char *orig_data = NULL;
    char **encoded_data = new char*[args->k], **encoded_parity = new char*[args->m];
    uint64_t encoded_fragment_len = chunks->at(0)->size();
    uint64_t decoded_data_len = 0;
    char *decoded_data = NULL;
    size_t frag_header_size =  sizeof(fragment_header_t);
    char **avail_frags = NULL;
    int num_avail_frags = 0;
    char *orig_data_ptr = NULL;
    int remaining = 0;
    
    for(int i = 0; i < args->k; i++){
        encoded_data[i] = new char[chunks->at(0)->size()];
    }
    
    for(int i = 0; i < args->m; i++){
        encoded_parity[i] = nullptr;
    }
    

    desc = liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, args);

    if (-EBACKENDNOTAVAIL == desc) {
        fprintf(stderr, "Backend library not available!\n");
        return;
    } else if ((args->k + args->m) > EC_MAX_FRAGMENTS) {
        assert(-EINVALIDPARAMS == desc);
        return;
    } else
        assert(desc > 0);
    
    printf("ABBBB\n");

//    orig_data = create_buffer(orig_data_size, 'x');
//    assert(orig_data != NULL);
//    rc = liberasurecode_encode(desc, orig_data, orig_data_size,
//            &encoded_data, &encoded_parity, &encoded_fragment_len);
//    assert(0 == rc);
//    orig_data_ptr = orig_data;
//    remaining = orig_data_size;
//    for (i = 0; i < args->k + args->m; i++)
//    {
//        int cmp_size = -1;
//        char *data_ptr = NULL;
//        char *frag = NULL;
//
//        frag = (i < args->k) ? encoded_data[i] : encoded_parity[i - args->k];
//        assert(frag != NULL);
//        fragment_header_t *header = (fragment_header_t*)frag;
//        assert(header != NULL);
//
//        fragment_metadata_t metadata = header->meta;
//        assert(metadata.idx == i);
//        assert(metadata.size == encoded_fragment_len - frag_header_size - metadata.frag_backend_metadata_size);
//        assert(metadata.orig_data_size == orig_data_size);
//        assert(metadata.backend_id == be_id);
//        assert(metadata.chksum_mismatch == 0);
//        data_ptr = frag + frag_header_size;
//        cmp_size = remaining >= metadata.size ? metadata.size : remaining;
//        // shss & libphazr doesn't keep original data on data fragments
//        if (be_id != EC_BACKEND_SHSS && be_id != EC_BACKEND_LIBPHAZR) {
//            assert(memcmp(data_ptr, orig_data_ptr, cmp_size) == 0);
//        }
//        remaining -= cmp_size;
//        orig_data_ptr += metadata.size;
//    }
    
    for(i = 0; i < args->k + args->m; i++){
        if(i < args->k){
            for(int j = 0; j < chunks->at(0)->size(); j++){
                encoded_data[i][j] = chunks->at(i)->at(j);
            }
        }
//        else{
//            for(int j = 0; j < chunks->at(0)->size(); j++){
//                encoded_parity[i][j] = chunks->at(i)->at(j);
//            }
//        }
    }

    
    int *skip = new int[args->k + args->m];
    for(int i = 0; i < args->k + args->m; i++){
        skip[i] = 1;
    }
    
    for(int i = 0; i < args->k; i++){
        skip[i] = 0;
    }
    
    num_avail_frags = create_frags_array(&avail_frags, encoded_data,
                                         encoded_parity, args, skip);
    assert(num_avail_frags > 0);
    rc = liberasurecode_decode(desc, avail_frags, num_avail_frags,
                               chunks->at(0)->size(), 1,
                               &decoded_data, &decoded_data_len);
    assert(0 == rc);
//    assert(decoded_data_len == orig_data_size);
//    assert(memcmp(decoded_data, orig_data, orig_data_size) == 0);

//    rc = liberasurecode_encode_cleanup(desc, encoded_data, encoded_parity);
//    assert(rc == 0);

    
    data->clear();
    for(int i = 0; i < decoded_data_len; i++){
        data->push_back(decoded_data[i]);
    }
    
    rc = liberasurecode_decode_cleanup(desc, decoded_data);
    assert(rc == 0);

    assert(0 == liberasurecode_instance_destroy(desc));
    free(orig_data);
    free(avail_frags);
    
    for(int i = 0; i < args->k; i++){
        delete encoded_data[i];
    }
    
    delete encoded_data;
    delete encoded_parity;
    
}

uint32_t CAS_Client::put(std::string key, std::string value, Placement &p, bool insert){
    
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
        timestamp = this->get_timestamp(&key);
    }
    // Join the encoder thread
    encoder.join();
    
    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    
    // prewrite
    int i = 0;
    
    for(std::vector<DC*>::iterator it = p.Q2.begin();
            it != p.Q2.end(); it++){
        std::thread th(_put, &key, chunks[i], &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class);
        th.detach();
//        threads.push_back(&th);
        i++;
    }
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q2.size()){
        cv.wait(lock);
    }
    lock.unlock();
    
    // fin tag
    counter = 0;
//    threads.clear(); // ToDo: Do we need this?
    for(std::vector<DC*>::iterator it = p.Q3.begin();
            it != p.Q3.end(); it++){
        std::thread th(_put_fin, &key, &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class);
        th.detach();
//        threads.push_back(&th);
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

void _get(std::string *key, std::vector<std::string*> *chunks, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class){
    
//    printf("AAAA\n");
    
    DPRINTF(DEBUG_CAS_Client, "started.\n");
    
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
   
//    printf("sutck here");
    //ToDo: take care of connect by putting a timeout over it
    if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){ 
        printf("\nConnection Failed \n"); 
        return; 
    }
    
//    printf("I am connected\n");

    
//    std::string data =
//        "{\n"\
//            "\t\"method\": get\n"\
//            "\t\"key\": \""+ (*key) + "\"\n"\
//            "\t\"timestamp\": \""+ timestamp->get_string() + "\"\n"\
//            "\t\"class\": \"" + current_class + "\"\n"\
//            "\t\"value_required: \"" + "True" + "\"\n"\
//        "}";
    
    std::string data = "get+:--:+";
    data += *key;
    data += "+:--:+";
    data += timestamp->get_string();
    data += "+:--:+";
    data += "Viveck_1";
    data += "+:--:+";
    data += "True";
    
    
    // ToDo: test these functions
    send_msg(sock, data);
    
//    sock.settimeout(this->timeout_per_request);

    uint8_t buf[640000];
    int buf_size = read(sock, buf, 640000);
//    printf("shahrooz:\n");
//    for(int i = 0; i < buf_size; i++){
//        printf("%c", buf[i]);
//    }
//    printf("\n\n");
    
    
//    return;
    
    
    
    std::string tmp((const char*)buf, buf_size);
    
    std::size_t pos = tmp.find("+:--:+");
    pos = tmp.find("+", pos + 2) + 1;
//    std::size_t start_index = tmp.find("\"", pos);
//    if(start_index == std::string::npos){
//        return;
//    }
//    start_index++;
//    std::size_t end_index = tmp.find("\"", start_index);
    
    std::string data_portion = tmp.substr(pos);
    
//    std::size_t dash_pos = timestamp_str.find("-");
    
//    DPRINTF(DEBUG_CAS_Client, "time: %s\n", timestamp_str.c_str());
    
    // make client_id and time regarding the received message
//    uint32_t client_id_ts = stoi(timestamp_str.substr(0, dash_pos));
//    uint32_t time_ts = stoi(timestamp_str.substr(dash_pos + 1));
//    
//    Timestamp* t = new Timestamp(client_id_ts, time_ts);
    
    std::unique_lock<std::mutex> lock(*mutex);
//    tss->push_back(t);
    chunks->push_back(new std::string(data_portion));
    (*counter)++;
    cv->notify_one();
    
    // ToDo: read the status of server
//    uint8_t buf[640000];
//    read(sock, buf, 640000);
//    sock.close();
    
//    std::unique_lock<std::mutex> lock(*mutex);
//    (*counter)++;
//    cv->notify_one();
    
    DPRINTF(DEBUG_CAS_Client, "finished successfully.\n");
    
    return;
}

uint32_t CAS_Client::get(std::string key, std::string &value, Placement &p){
    
    value.clear();
    
    Timestamp *timestamp = nullptr;
    timestamp = this->get_timestamp(&key);
    
//    printf("%s\n", timestamp->get_string().c_str());

    // phase 2
    std::vector <std::string*> chunks;
//    for(int i = 0; i < p.Q4.size(); i++){
//        chunks.push_back(new std::string(""));
//    }
    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    int i = 0;
    
//    printf("bbbb\n");
    
    for(std::vector<DC*>::iterator it = p.Q4.begin();
            it != p.Q4.end(); it++){
        std::thread th(_get, &key, &chunks, &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class);
        th.detach();
//        threads.push_back(&th);
        i++;
    }
//    printf("aaaaaa\n");
    
    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q4.size()){
        cv.wait(lock);
    }
    lock.unlock();
//    printf("aaaaaa\n");
    //ToDo: you should kill other threads here, if you want to be more efficient, and uncomment the code below.
//    for(int i = 0; i < chunks.size(); i++){
//        if(chunks[i]->size() == 0){
//            delete chunks[i];
//            chunks.erase(chunks.begin() + i);
//            i--;
//        }
//    }
    
    if(chunks.size() < p.k){
        DPRINTF(DEBUG_CAS_Client, "chunks.size() < p.m\n");
    }
    
    // Decode
    struct ec_args null_args;
    null_args.k = p.k;
    null_args.m = p.m - p.k;
    null_args.w = 16; // ToDo: what must it be?
    null_args.ct = CHKSUM_NONE;
    
    printf("AAAAA\n");
    
    decode(&value, &chunks, &null_args);
    
    printf("TEST VAL: %s\n", value.c_str());
    
    
    return S_OK;
}
