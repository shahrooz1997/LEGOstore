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

#include <sys/time.h>

char fmt_cas[64];
char buf_cas[64];
struct timeval tv_cas;
struct tm *tm22_cas;

//TODO: Ask more servers for timestamp if last attempt didn't work.

CAS_Client::CAS_Client(Properties &prop, uint32_t client_id, int desc_l) {
    this->current_class = "CAS";
    this->id = client_id;
    this->prop = prop;
    char name[20];
    this->operation_id = 0;
    name[0] = 'l';
    name[1] = 'o';
    name[2] = 'g';
    name[3] = 's';
    name[4] = '/';

    sprintf(&name[5], "%u.txt", client_id);
    this->log_file = fopen(name, "w");
    this->desc = -1;
    this->desc = desc_l;
    //this->destroy_desc = 1;
}


CAS_Client::~CAS_Client() {
    fclose(this->log_file);
    DPRINTF(DEBUG_CAS_Client, "cliend with id \"%u\" has been destructed.\n", this->id);
}

uint32_t CAS_Client::get_operation_id(){
    return operation_id;
}

void _get_timestamp(std::string *key, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter, Server *server,
                    std::string current_class, std::vector<Timestamp*> *tss,
                    uint32_t operation_id, CAS_Client *doer){
    DPRINTF(DEBUG_CAS_Client, "started.\n");
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
    data.push_back("get_timestamp");
    data.push_back(key2);
    data.push_back(current_class);
    DataTransfer::sendMsg(sock, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;
    if(DataTransfer::recvMsg(sock, recvd) == 1){ // data must have the timestamp.
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
            printf("get timestamp, data received is %s\n", data[1].c_str());
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
            if(doer->get_operation_id() == operation_id){

                tss->push_back(t);
                (*counter)++;
                cv->notify_one();
            }
            else{
                delete t;
            }
        }
    }

    close(sock);
    return;
}

Timestamp* CAS_Client::get_timestamp(std::string *key, Placement &p){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    std::vector<Timestamp*> tss;
    Timestamp *ret = nullptr;

    for(std::vector<DC*>::iterator it = p.Q1.begin();
            it != p.Q1.end(); it++){
        std::thread th(_get_timestamp, key, &mtx, &cv, &counter,
                (*it)->servers[0], this->current_class, &tss, this->operation_id, this);
        th.detach();
    }

    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q1.size()){
        cv.wait(lock);
    }

    ret = new Timestamp(Timestamp::max_timestamp(tss));
    for(std::vector<Timestamp*>::iterator it = tss.begin(); it != tss.end(); it++){
        delete *it;
    }

    this->operation_id++;
    lock.unlock();
    DPRINTF(DEBUG_CAS_Client, "finished successfully.\n");

    return ret;
}

void _put(std::string *key, std::string *value, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class, uint32_t operation_id, CAS_Client *doer){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

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
    data.push_back("put");

    data.push_back(key2);
    data.push_back(value2);
    data.push_back(timestamp2.get_string());
    data.push_back(current_class);
//    std::cout<< "AAAAA: Sending Value  _PUT"<<(*value).size() << "value is "<< (*value) <<std::endl;
    if((value2).empty()){
	printf("WARNING!!! SENDING EMPTY STRING \n");
    }
    DataTransfer::sendMsg(sock,DataTransfer::serialize(data));

    data.clear();
    std::string recvd;
    DataTransfer::recvMsg(sock, recvd); // data must have the timestamp.
    data =  DataTransfer::deserialize(recvd);

    // Todo: parse data

    std::unique_lock<std::mutex> lock(*mutex);
    if(doer->get_operation_id() == operation_id){

        (*counter)++;
        cv->notify_one();
//        printf("AAA\n");
    }

    DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server->port);
    close(sock);

    return;
}

void _put_fin(std::string *key, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class, uint32_t operation_id, CAS_Client *doer){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

    std::string key2 = *key;
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
    data.push_back("put_fin");
    data.push_back(key2);
    data.push_back(timestamp2.get_string());
    data.push_back(current_class);
    DataTransfer::sendMsg(sock, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;
    DataTransfer::recvMsg(sock, recvd); // data must have the timestamp.
    data = DataTransfer::deserialize(recvd);

    // Todo: parse data

    std::unique_lock<std::mutex> lock(*mutex);
    if(doer->get_operation_id() == operation_id){
        (*counter)++;
        cv->notify_one();
//        printf("AAA\n");
    }

    DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server->port);
    close(sock);

    return;
}

void encode(const std::string * const data, std::vector <std::string*> *chunks,
                        struct ec_args * const args, int desc){

    int rc = 0;
    //int desc = -1;
    char *orig_data = NULL;
    char **encoded_data = NULL, **encoded_parity = NULL;
    uint64_t encoded_fragment_len = 0;

    //desc = liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, args);

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
        //int cmp_size = -1;
        //char *data_ptr = NULL;
        char *frag = NULL;

        frag = (i < args->k) ? encoded_data[i] : encoded_parity[i - args->k];
        assert(frag != NULL);
        chunks->push_back(new std::string(frag, encoded_fragment_len));
    }

    rc = liberasurecode_encode_cleanup(desc, encoded_data, encoded_parity);
    assert(rc == 0);

    //assert(0 == liberasurecode_instance_destroy(desc));

    return;
}

int create_frags_array(char ***array,
                              char **data,
                              char **parity,
                              struct ec_args *args,
                              int *skips){
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
            //printf("%d skipped1\n", i);
            continue;
        }
        *ptr++ = data[i];
        num_frags++;
    }
    //add parity frags
    for (i = 0; i < args->m; i++) {
        if (parity[i] == NULL || skips[i + args->k] == 1) {
            //printf("%d skipped2\n", i);
            continue;
        }
        *ptr++ = parity[i];
        num_frags++;
    }
out:
    return num_frags;
}

void decode(std::string *data, std::vector <std::string*> *chunks,
                        struct ec_args * const args, int desc){

    int i = 0;
    int rc = 0;
    //int desc = -1;
    char *orig_data = NULL;
    char **encoded_data = new char*[args->k], **encoded_parity = new char*[args->m];
    uint64_t decoded_data_len = 0;
    char *decoded_data = NULL;
    char **avail_frags = NULL;
    int num_avail_frags = 0;

    for(int i = 0; i < args->k; i++){
        encoded_data[i] = new char[chunks->at(0)->size()];
    }

    for(int i = 0; i < args->m; i++){
        encoded_parity[i] = nullptr;
    }

    DPRINTF(DEBUG_CAS_Client, "The chunk size DECODE is %lu\n", chunks->at(0)->size());

    //desc = liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, args);

    if (-EBACKENDNOTAVAIL == desc) {
        fprintf(stderr, "Backend library not available!\n");
        return;
    } else if ((args->k + args->m) > EC_MAX_FRAGMENTS) {
        assert(-EINVALIDPARAMS == desc);
        return;
    } else
        assert(desc > 0);

    for(i = 0; i < args->k + args->m; i++){
        if(i < args->k){
            for(uint j = 0; j < chunks->at(0)->size(); j++){
                encoded_data[i][j] = chunks->at(i)->at(j);
            }
        }
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

    data->clear();
    for(uint i = 0; i < decoded_data_len; i++){
        data->push_back(decoded_data[i]);
    }

    rc = liberasurecode_decode_cleanup(desc, decoded_data);
    assert(rc == 0);

    //assert(0 == liberasurecode_instance_destroy(desc));
    free(orig_data);
    free(avail_frags);

    for(int i = 0; i < args->k; i++){
        delete[] encoded_data[i];
    }

    delete[] skip;
    delete[] encoded_data;
    delete[] encoded_parity;
}

uint32_t CAS_Client::put(std::string key, std::string value, Placement &p, bool insert){

//    gettimeofday (&tv_cas, NULL);
//    tm22_cas = localtime (&tv_cas.tv_sec);
//    strftime (fmt_cas, sizeof (fmt_cas), "%H:%M:%S:%%06u", tm22_cas);
//    snprintf (buf_cas, sizeof (buf_cas), fmt_cas, tv_cas.tv_usec);
//    fprintf(this->log_file, "%s write invoke %s\n", buf_cas, value.c_str());

    std::vector <std::string*> chunks;
    struct ec_args null_args;
    null_args.k = p.k;
    null_args.m = p.m - p.k;
    null_args.w = 16;
    null_args.ct = CHKSUM_NONE;

    DPRINTF(DEBUG_CAS_Client, "The value to be stored is %s \n", value.c_str());

    std::thread encoder(encode, &value, &chunks, &null_args, this->desc);

    Timestamp *timestamp = nullptr;
    Timestamp *tmp = nullptr;

    if(insert){ // This is a new key
        timestamp = new Timestamp(this->id);
    }
    else{
        tmp = this->get_timestamp(&key, p);
        timestamp = new Timestamp(tmp->increase_timestamp(this->id));
        delete tmp;
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
//	printf("The port is: %uh", (*it)->servers[0]->port);

        std::thread th(_put, &key, chunks[i], &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class, this->operation_id, this);
        th.detach();
        DPRINTF(DEBUG_CAS_Client, "Issue Q2 request to key: %s and timestamp: %s  chunk_size :%lu  chunks: ", key.c_str(), timestamp->get_string().c_str(), chunks[i]->size());
        // for (auto& el : *chunks[i])
  	    //      printf("%02hhx", el);
        // std::cout << '\n';
        i++;
    }

    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q2.size()){
        cv.wait(lock);
    }

    this->operation_id++;
    lock.unlock();

    // fin tag
    counter = 0;
    for(std::vector<DC*>::iterator it = p.Q3.begin();
            it != p.Q3.end(); it++){
        std::thread th(_put_fin, &key, &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class, this->operation_id, this);
        th.detach();
        DPRINTF(DEBUG_CAS_Client, "Issue Q3 request to key: %s \n", key.c_str());
    }



    std::unique_lock<std::mutex> lock2(mtx);
    while(counter < p.Q3.size()){
        cv.wait(lock2);
    }


    this->operation_id++;
    lock2.unlock();

//    gettimeofday (&tv_cas, NULL);
//    tm22_cas = localtime (&tv_cas.tv_sec);
//    strftime (fmt_cas, sizeof (fmt_cas), "%H:%M:%S:%%06u", tm22_cas);
//    snprintf (buf_cas, sizeof (buf_cas), fmt_cas, tv_cas.tv_usec);
//    fprintf(this->log_file, "%s write ok %s\n", buf_cas, value.c_str());

    for(uint i = 0; i < chunks.size(); i++){
        delete chunks[i];
    }
    delete timestamp;

    return S_OK;
}

void _get(std::string *key, std::vector<std::string*> *chunks, std::mutex *mutex,
                    std::condition_variable *cv, uint32_t *counter,
                    Server *server, Timestamp* timestamp,
                    std::string current_class, uint32_t operation_id, CAS_Client *doer){
    DPRINTF(DEBUG_CAS_Client, "started.\n");

    std::string key2 = *key;
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
    data.push_back("get");
    data.push_back(key2);
    data.push_back(timestamp->get_string());
    data.push_back(current_class);
    //data.push_back("True");
    DataTransfer::sendMsg(sock, DataTransfer::serialize(data));

    data.clear();

    std::string recvd;

    if(DataTransfer::recvMsg(sock, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        if(data[0] == "OK"){
            std::string data_portion = data[1];

            DPRINTF(DEBUG_CAS_Client, " chunk received for key : %s and timestamp :%s   chunk_size :%lu  is ", key2.c_str(), timestamp2.get_string().c_str(), data_portion.size());
            std::unique_lock<std::mutex> lock(*mutex);
            if(doer->get_operation_id() == operation_id){

                chunks->push_back(new std::string(data_portion));
        //        tss->push_back(t);
                (*counter)++;
                cv->notify_one();
            }
        }
    }

    close(sock);
    DPRINTF(DEBUG_CAS_Client, "finished successfully.\n");

    return;
}

uint32_t CAS_Client::get(std::string key, std::string &value, Placement &p){

//    gettimeofday (&tv_cas, NULL);
//    tm22_cas = localtime (&tv_cas.tv_sec);
//    strftime (fmt_cas, sizeof (fmt_cas), "%H:%M:%S:%%06u", tm22_cas);
//    snprintf (buf_cas, sizeof (buf_cas), fmt_cas, tv_cas.tv_usec);
//    fprintf(this->log_file, "%s read invoke nil\n", buf_cas);


    value.clear();

    Timestamp *timestamp = nullptr;
    timestamp = this->get_timestamp(&key, p);

    // phase 2
    std::vector <std::string*> chunks;

    uint32_t counter = 0;
    std::mutex mtx;
    std::condition_variable cv;
    int i = 0;

    for(std::vector<DC*>::iterator it = p.Q4.begin();
            it != p.Q4.end(); it++){
        std::thread th(_get, &key, &chunks, &mtx, &cv, &counter,
                (*it)->servers[0], timestamp, this->current_class, this->operation_id, this);
        th.detach();
        DPRINTF(DEBUG_CAS_Client, "Issue Q4 request for key :%s \n", key.c_str());
        i++;
    }

    std::unique_lock<std::mutex> lock(mtx);
    while(counter < p.Q4.size()){
        cv.wait(lock);
    }

    this->operation_id++;
    lock.unlock();

    if(chunks.size() < p.k){
        DPRINTF(DEBUG_CAS_Client, "chunks.size() < p.m\n");
    }

    // Decode
    struct ec_args null_args;
    null_args.k = p.k;
    null_args.m = p.m - p.k;
    null_args.w = 16; // ToDo: what must it be?
    null_args.ct = CHKSUM_NONE;
//    gettimeofday (&tv_cas, NULL);
//    tm22_cas = localtime (&tv_cas.tv_sec);
//    strftime (fmt_cas, sizeof (fmt_cas), "%H:%M:%S:%%06u", tm22_cas);
//    snprintf (buf_cas, sizeof (buf_cas), fmt_cas, tv_cas.tv_usec);
//    fprintf(this->log_file, "%s read ok %s\n", buf_cas, value.c_str());



    decode(&value, &chunks, &null_args, this->desc);

    DPRINTF(DEBUG_CAS_Client,"Received value for key : %s and timestamp is :%s  is: %s\n", key.c_str(), timestamp->get_string().c_str(), value.c_str());

    for(auto it: chunks){
        delete it;
    }
    delete timestamp;
    return S_OK;
}
