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
#include <future>
#include <unordered_set>

using std::string;

int Reconfig::start_reconfig(Properties *prop, GroupConfig &old_config, GroupConfig &new_config, int key_index, int desc){

    Timestamp *ret_ts = nullptr;
    std::string *ret_v = nullptr;
    Reconfig::send_reconfig_query(prop, old_config, key_index, ret_ts, ret_v);
    if(old_config.placement_p->protocol == "CAS"){
        Reconfig::send_reconfig_finalize(prop, old_config, key_index, ret_ts, ret_v, desc);
    }
    //Reconfig::send_reconfig_write(new_config, key_index, ret_ts, ret_v, desc);

    // Update local meta data at this point then make a call to send_reconfig_finish function

    // Call finish reconfig

    return 0;
}


void _send_reconfig_query(std::promise<strVec> &&prm, std::string key, Server *server,
                            std::string current_class){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");

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
    data.push_back(key);
    data.push_back(current_class);
    DataTransfer::sendMsg(sock, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;

    if(DataTransfer::recvMsg(sock, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        prm.set_value(std::move(data));
    }

    close(sock);
    return;
}

int Reconfig::send_reconfig_query(Properties *prop, GroupConfig &old_config, int key_index, Timestamp *ret_ts, std::string *ret_v){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");

    std::vector<Timestamp> tss;
    std::vector<string*> vs;
    std::unordered_set<uint32_t> old_servers;
    std::vector<std::future<strVec> > responses;

    Placement *p = old_config.placement_p;
    old_servers.insert(p->Q1.begin(), p->Q1.end());
    old_servers.insert(p->Q2.begin(), p->Q2.end());
    old_servers.insert(p->Q3.begin(), p->Q3.end());
    old_servers.insert(p->Q4.begin(), p->Q4.end());

    if(old_config.placement_p->protocol == "ABD"){

    }
    else if(old_config.placement_p->protocol == "CAS") {

        for(auto it = old_servers.begin();it != old_servers.end(); it++){

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(_send_reconfig_query, std::move(prm), old_config.keys[key_index],
                            prop->datacenters[*it]->servers[0], "CAS").detach();
        }

        uint32_t max_val = std::max(old_servers.size() - p->f, old_servers.size() - p->Q3.size() + 1);
        max_val = std::max(max_val, (uint32_t)(old_servers.size() - p->Q4.size() + 1));

        //POll all the values until you find the requisite responses
        // This is a busy loop, but it's not controller so it's fine
        while(max_val){
            for(auto &it:responses){
                if(it.valid() && it.wait_for(std::chrono::milliseconds(10)) == std::future_status::ready){

                    strVec data = it.get();

                    if(data[0] == "OK"){
                        printf("reconfig query, data received is %s\n", data[1].c_str());
                        std::string timestamp_str = data[1];

                        std::size_t dash_pos = timestamp_str.find("-");
                        if(dash_pos >= timestamp_str.size()){
                            std::cerr << "Substr violated !!!!!!!" << timestamp_str <<std::endl;
                            assert(0);
                        }

                        // make client_id and time regarding the received message
                        uint32_t client_id_ts = stoi(timestamp_str.substr(0, dash_pos));
                        uint32_t time_ts = stoi(timestamp_str.substr(dash_pos + 1));

                        tss.emplace_back(client_id_ts, time_ts);
                    }
                    //reponses.erase(it);
                    break;
                }
            }
            max_val--;
        }

        ret_ts = new Timestamp(Timestamp::max_timestamp2(tss));

    }

    DPRINTF(DEBUG_RECONFIG_CONTROL, "finished successfully.\n");

    return S_OK;
}


static void encode(const std::string * const data, std::vector <std::string*> *chunks,
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

static int create_frags_array(char ***array,
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

static void decode(std::string *data, std::vector <std::string*> *chunks,
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


void _send_reconfig_finalize(std::promise<strVec> &&prm, std::string key, Timestamp timestamp,
                    Server *server, std::string current_class){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");

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
    data.push_back("reconfig_finalize");
    data.push_back(key);
    data.push_back(timestamp.get_string());
    data.push_back(current_class);
    DataTransfer::sendMsg(sock, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;

    if(DataTransfer::recvMsg(sock, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        prm.set_value(std::move(data));
    }

    close(sock);
    return;
}

int Reconfig::send_reconfig_finalize(Properties *prop, GroupConfig &old_config, int key_index,
                                Timestamp *ret_ts, std::string *ret_v, int desc){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");

    std::vector<std::future<strVec> > responses;
    std::vector <std::string*> chunks;

    Placement *p = old_config.placement_p;

    for(auto it = p->Q4.begin(); it != p->Q4.end(); it++){
        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(_send_reconfig_finalize, std::move(prm), old_config.keys[key_index], *ret_ts,
                prop->datacenters[*it]->servers[0], p->protocol).detach();
    }

    for(auto &it:responses){
        strVec data = it.get();

        if(data[0] == "OK"){
            chunks.push_back(new std::string(data[1]));
        }else{
            //TODO::Operation fail, send msg to other servers
        }
    }

    if(chunks.size() < p->k){
        DPRINTF(DEBUG_CAS_Client, "chunks.size() < p.m\n");
    }

    // Decode
    struct ec_args null_args;
    null_args.k = p->k;
    null_args.m = p->m - p->k;
    null_args.w = 16; // ToDo: what must it be?
    null_args.ct = CHKSUM_NONE;
//    gettimeofday (&tv_cas, NULL);
//    tm22_cas = localtime (&tv_cas.tv_sec);
//    strftime (fmt_cas, sizeof (fmt_cas), "%H:%M:%S:%%06u", tm22_cas);
//    snprintf (buf_cas, sizeof (buf_cas), fmt_cas, tv_cas.tv_usec);
//    fprintf(this->log_file, "%s read ok %s\n", buf_cas, value.c_str());


    string value;
    decode(&value, &chunks, &null_args, desc);

    for(auto it: chunks){
        delete it;
    }

    //delete ret_ts;

    ret_v = new std::string(value);

    DPRINTF(DEBUG_RECONFIG_CONTROL,"Received value for key is: %s\n", value.c_str());
    return S_OK;
}

// void _send_reconfig_write(std::string *key, std::mutex *mutex,
//                     std::condition_variable *cv, uint32_t *counter, Server *server,
//                     Timestamp* timestamp, std::string *value, bool is_cas,
//                     uint32_t operation_id){
//     DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");
//     std::string key2 = *key;
//     Timestamp timestamp2 = *timestamp;
//     std::string vaule2 = *value;
//     int sock = 0;
//     struct sockaddr_in serv_addr;
//     if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
//         printf("\n Socket creation error \n");
//         return;
//     }
//     serv_addr.sin_family = AF_INET;
//     serv_addr.sin_port = htons(server->port);
//     std::string ip_str = server->ip;
//     //std::string ip_str = convert_ip_to_string(server->ip);
//
//     if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){
//         printf("\nInvalid address/ Address not supported \n");
//         return;
//     }
//
//     if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
//         printf("\nConnection Failed \n");
//         return;
//     }
//
//     strVec data;
//     data.push_back("write_config");
//     data.push_back(key2);
//     data.push_back(timestamp2.get_string());
//     data.push_back(vaule2);
//     if(is_cas)
//         data.push_back("fin");
//     DataTransfer::sendMsg(sock, DataTransfer::serialize(data));
//
//     data.clear();
//     std::string recvd;
//     DataTransfer::recvMsg(sock, recvd); // data must have the timestamp.
//     data =  DataTransfer::deserialize(recvd);
//
//     if(data[0] == "OK"){
//         printf("_send_reconfig_write\n");
//
//
// //        std::string data_portion = data[1];
//
//
//         std::unique_lock<std::mutex> lock(*mutex);
//         if(Reconfig::get_instance()->get_operation_id() == operation_id){
//
// //            chunks->push_back(new std::string(data_portion));
//     //        tss->push_back(t);
//             (*counter)++;
//             cv->notify_one();
//         }
//     }
//
//     close(sock);
//     return;
// }
//
//
// int Reconfig::send_reconfig_write(GroupConfig &new_config, int key_index, Timestamp *ret_ts, std::string *ret_v){
//
//     Placement &p = *(new_config.placement_p);
//
//     std::vector <std::string*> chunks;
//     struct ec_args null_args;
//     null_args.k = p.k;
//     null_args.m = p.m - p.k;
//     null_args.w = 16;
//     null_args.ct = CHKSUM_NONE;
//
// //    DPRINTF(DEBUG_CAS_Client, "The value to be stored is %s \n", value.c_str());
//
//
//
//     Timestamp *timestamp = ret_ts;
//
//     uint32_t counter = 0;
//     std::mutex mtx;
//     std::condition_variable cv;
//     int i = 0;
//
//     if(new_config.placement_p->protocol == "ABD"){
//
//         for(std::vector<DC*>::iterator it = p.Q2.begin();
//                 it != p.Q2.end(); it++){
//     //	printf("The port is: %uh", (*it)->servers[0]->port);
//
//             std::thread th(_send_reconfig_write, &new_config.keys[key_index], &mtx, &cv, &counter,
//                     (*it)->servers[0], timestamp, ret_v, false, this->operation_id);
//             th.detach();
//
//             // for (auto& el : *chunks[i])
//                 //      printf("%02hhx", el);
//             // std::cout << '\n';
//             i++;
//         }
//
//         std::unique_lock<std::mutex> lock(mtx);
//         while(counter < p.Q2.size()){
//             cv.wait(lock);
//         }
//
//         this->operation_id++;
//         lock.unlock();
//
//
//     }
//     else{ // CAS
//
//         std::thread encoder(encode, ret_v, &chunks, &null_args, this->desc);
//
//         for(std::vector<DC*>::iterator it = p.Q2.begin();
//                 it != p.Q2.end(); it++){
//     //	printf("The port is: %uh", (*it)->servers[0]->port);
//
//             std::thread th(_send_reconfig_write, &new_config.keys[key_index], &mtx, &cv, &counter,
//                     (*it)->servers[0], timestamp, chunks[i], false, this->operation_id);
//             th.detach();
//
//             // for (auto& el : *chunks[i])
//                 //      printf("%02hhx", el);
//             // std::cout << '\n';
//             i++;
//         }
//
//         std::unique_lock<std::mutex> lock(mtx);
//         while(counter < p.Q2.size()){
//             cv.wait(lock);
//         }
//
//         this->operation_id++;
//         lock.unlock();
//         for(auto it: chunks){
//             delete it;
//         }
// //        delete timestamp;
//     }
//
//     DPRINTF(DEBUG_RECONFIG_CONTROL, "finished successfully.\n");
//
//     return S_OK;
// }
//
// void _send_reconfig_finish(std::string *key, std::mutex *mutex,
//                     std::condition_variable *cv, uint32_t *counter, Server *server,
//                     Timestamp* timestamp,
//                     uint32_t operation_id, GroupConfig *new_config){
//     DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");
//     std::string key2 = *key;
//     Timestamp timestamp2 = *timestamp;
//     GroupConfig new_config2 = *new_config;
//     int sock = 0;
//     struct sockaddr_in serv_addr;
//     if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
//         printf("\n Socket creation error \n");
//         return;
//     }
//     serv_addr.sin_family = AF_INET;
//     serv_addr.sin_port = htons(server->port);
//     std::string ip_str = server->ip;
//     //std::string ip_str = convert_ip_to_string(server->ip);
//
//     if(inet_pton(AF_INET, ip_str.c_str(), &serv_addr.sin_addr) <= 0){
//         printf("\nInvalid address/ Address not supported \n");
//         return;
//     }
//
//     if(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
//         printf("\nConnection Failed \n");
//         return;
//     }
//
//     //TODO:: fix this
//     // Send just placemrnt and that too based on ABD and CAS
//     char *ncp = (char*)new_config;
//     string ncs;
//     for(int i = 0; i < (int)sizeof(GroupConfig); i++){
//         ncs.push_back(ncp[i]);
//     }
//
//     strVec data;
//     data.push_back("finish_reconfig");
//     data.push_back(key2);
//     data.push_back(timestamp2.get_string());
//     data.push_back(ncs);
//     DataTransfer::sendMsg(sock, DataTransfer::serialize(data));
//
//     data.clear();
//     std::string recvd;
//     DataTransfer::recvMsg(sock, recvd); // data must have the timestamp.
//     data =  DataTransfer::deserialize(recvd);
//
//     if(data[0] == "OK"){
//         printf("_send_reconfig_write\n");
//
//         std::unique_lock<std::mutex> lock(*mutex);
//         if(Reconfig::get_instance()->get_operation_id() == operation_id){
//
// //            chunks->push_back(new std::string(data_portion));
//     //        tss->push_back(t);
//             (*counter)++;
//             cv->notify_one();
//         }
//     }
//
//     close(sock);
//     return;
// }
//
// int Reconfig::send_reconfig_finish(GroupConfig &old_config, GroupConfig &new_config, int key_index, Timestamp *ret_ts, std::string *ret_v){
//
//     DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");
//
//     ret_ts = this->__timestamp;
//
//     uint32_t counter = 0;
//     std::mutex mtx;
//     std::condition_variable cv;
// //    std::vector<Timestamp*> tss;
// //    std::vector<string*> vs;
//     int ret = 0;
//
//     std::vector<DC*> N;
//     Placement &p = *(old_config.placement_p);
//     for(std::vector<DC*>::iterator it = p.Q1.begin();
//             it != p.Q1.end(); it++){
//         if(std::find(N.begin(), N.end(), (*it)) == N.end()){
//             N.push_back((*it));
//         }
//     }
//     for(std::vector<DC*>::iterator it = p.Q2.begin();
//             it != p.Q2.end(); it++){
//         if(std::find(N.begin(), N.end(), (*it)) == N.end()){
//             N.push_back((*it));
//         }
//     }
//     for(std::vector<DC*>::iterator it = p.Q3.begin();
//             it != p.Q3.end(); it++){
//         if(std::find(N.begin(), N.end(), (*it)) == N.end()){
//             N.push_back((*it));
//         }
//     }
//     for(std::vector<DC*>::iterator it = p.Q4.begin();
//             it != p.Q4.end(); it++){
//         if(std::find(N.begin(), N.end(), (*it)) == N.end()){
//             N.push_back((*it));
//         }
//     }
//
//     int i = 0;
//
//
//     for(std::vector<DC*>::iterator it = N.begin();
//             it != N.end(); it++){
//         std::thread th(_send_reconfig_finish, &old_config.keys[key_index], &mtx, &cv, &counter,
//                 (*it)->servers[0], ret_ts, this->operation_id, &new_config);
//         th.detach();
// //        DPRINTF(DEBUG_CAS_Client, "Issue Q4 request for key :%s \n", key.c_str());
//         i++;
//     }
//
//     std::unique_lock<std::mutex> lock(mtx);
//     while(counter < p.Q4.size()){
//         cv.wait(lock);
//     }
//
//     this->operation_id++;
//
//     lock.unlock();
//
//     //delete ret_ts;
//
//
//     DPRINTF(DEBUG_RECONFIG_CONTROL,"finished.\n");
//     return ret;
// }
//
// Reconfig::Reconfig() {
// }
//
// Reconfig::~Reconfig() {
//     delete this->instance;
//     this->instance = nullptr;
// }
