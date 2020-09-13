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
#include <vector>


template <typename T>
static void set_intersection(Placement *p, std::unordered_set<T> &res){
    res.insert(p->Q1.begin(), p->Q1.end());
    res.insert(p->Q2.begin(), p->Q2.end());
    res.insert(p->Q3.begin(), p->Q3.end());
    res.insert(p->Q4.begin(), p->Q4.end());
}

namespace CAS_thread_helper{
    void _get_timestamp(std::promise<strVec> &&prm, std::string key, Server *server,
                        std::string current_class, uint32_t conf_id){

        DPRINTF(DEBUG_CAS_Client, "started.\n");

        strVec data;

        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            prm.set_value(std::move(data));
            return;
        }

        data.push_back("get_timestamp");
        data.push_back(key);
        data.push_back(current_class);
        data.push_back(std::to_string(conf_id));
        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        data.clear();
        std::string recvd;

        // If the socket recv itself fails, then 'promise' value will not be made available
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data =  DataTransfer::deserialize(recvd);
            prm.set_value(std::move(data));
        }
        else{
            data.clear();
            prm.set_value(std::move(data));
        }
        
        DPRINTF(DEBUG_ABD_Client, "finished.\n");

        return;
    }
    
    void _put(std::promise<strVec> &&prm, std::string key, std::string value, Timestamp timestamp,
                        Server *server, std::string current_class, uint32_t conf_id){

        DPRINTF(DEBUG_CAS_Client, "started.\n");
        
        strVec data;

        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            prm.set_value(std::move(data));
            return;
        }

        data.push_back("put");
        data.push_back(key);
        data.push_back(timestamp.get_string());
        data.push_back(value);
        data.push_back(current_class);
        data.push_back(std::to_string(conf_id));

        if((value).empty()){
            DPRINTF(DEBUG_CAS_Client, "WARNING!!! SENDING EMPTY STRING TO SERVER.\n");
        }
        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        data.clear();
        std::string recvd;


        // If the socket recv itself fails, then 'promise' value will not be made available
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data =  DataTransfer::deserialize(recvd);
            prm.set_value(std::move(data));
        }
        else{
            data.clear();
            prm.set_value(std::move(data));
        }

        DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server->port);

        return;
    }
    
    void _put_fin(std::promise<strVec> &&prm, std::string key, Timestamp timestamp,
                        Server *server, std::string current_class, uint32_t conf_id){

        DPRINTF(DEBUG_CAS_Client, "started.\n");

        strVec data;

        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            prm.set_value(std::move(data));
            return;
        }

        data.push_back("put_fin");
        data.push_back(key);
        data.push_back(timestamp.get_string());
        data.push_back(current_class);
        data.push_back(std::to_string(conf_id));
        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        data.clear();
        std::string recvd;

        // If the socket recv itself fails, then 'promise' value will not be made available
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data =  DataTransfer::deserialize(recvd);
            prm.set_value(std::move(data));
        }
        else{
            data.clear();
            prm.set_value(std::move(data));
        }
        
        DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server->port);

        return;
    }

    
    void _get(std::promise<strVec> &&prm, std::string key, Timestamp timestamp,
                    Server *server, std::string current_class, uint32_t conf_id){

        DPRINTF(DEBUG_CAS_Client, "started.\n");

        strVec data;

        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            prm.set_value(std::move(data));
            return;
        }

        data.push_back("get");
        data.push_back(key);
        data.push_back(timestamp.get_string());
        data.push_back(current_class);
        data.push_back(std::to_string(conf_id));
        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        data.clear();
        std::string recvd;

        // If the socket recv itself fails, then 'promise' value will not be made available
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data =  DataTransfer::deserialize(recvd);
            prm.set_value(std::move(data));
        }
        else{
            data.clear();
            prm.set_value(std::move(data));
        }

        DPRINTF(DEBUG_CAS_Client, "finished successfully.\n");

        return;
    }
}

namespace liberasure{
    
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
        
//        for(int i = 0; i < args->m; i++){
//            char *frag = encoded_parity[i];
//            assert(frag != NULL);
//            chunks->push_back(new std::string(frag, encoded_fragment_len));
//        }
//        
//        for(int i = 0; i < args->k; i++){
//            char *frag = encoded_data[i];
//            assert(frag != NULL);
//            chunks->push_back(new std::string(frag, encoded_fragment_len));
//        }

        rc = liberasurecode_encode_cleanup(desc, encoded_data, encoded_parity);
        assert(rc == 0);

        //assert(0 == liberasurecode_instance_destroy(desc));

        return;
    }

    int create_frags_array(char ***array,
                        char **data, char **parity, struct ec_args *args, int *skips){
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
//            DPRINTF(DEBUG_CAS_Client, "num_frags is %d.\n", num_frags);
        }
        //add parity frags
        for (i = 0; i < args->m; i++) {
            if (parity[i] == NULL || skips[i + args->k] == 1) {
                //printf("%d skipped2\n", i);
                continue;
            }
            *ptr++ = parity[i];
            num_frags++;
//            DPRINTF(DEBUG_CAS_Client, "1111num_frags is %d.\n", num_frags);
        }
    out:
//        DPRINTF(DEBUG_CAS_Client, "2222num_frags is %d.\n", num_frags);
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
            encoded_data[i] = new char[chunks->at(i)->size()];
        }

        for(int i = 0; i < args->m; i++){
            encoded_parity[i] = nullptr;
        }

//        DPRINTF(DEBUG_CAS_Client, "number of available chunks %lu\n", chunks->size());
//        for(int i = 0; i < chunks->size(); i++){
//            DPRINTF(DEBUG_CAS_Client, "chunk addr %p\n", chunks->at(i));
//        }
//        DPRINTF(DEBUG_CAS_Client, "The chunk size DECODE is %lu\n", chunks->at(0)->size());

        //desc = liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, args);

        if (-EBACKENDNOTAVAIL == desc) {
            fprintf(stderr, "Backend library not available!\n");
            return;
        } else if ((args->k + args->m) > EC_MAX_FRAGMENTS) {
            assert(-EINVALIDPARAMS == desc);
            return;
        } else
            assert(desc > 0);

//        for(i = 0; i < args->k + args->m; i++){
//            if(i < args->k){
//                for(uint j = 0; j < chunks->at(0)->size(); j++){
//                    encoded_data[i][j] = chunks->at(i)->at(j);
//                }
//            }
//        }

        for(i = 0; i < args->k; i++){
            for(uint j = 0; j < chunks->at(i)->size(); j++){
                encoded_data[i][j] = chunks->at(i)->at(j);
            }
        }
        
        int *skip = new int[args->k + args->m];
        for(int i = 0; i < args->k; i++){
            skip[i] = 0;
        }

        for(int i = args->k; i < args->k + args->m; i++){
            skip[i] = 1;
        }

        num_avail_frags = create_frags_array(&avail_frags, encoded_data,
                                             encoded_parity, args, skip);
        assert(num_avail_frags > 0);
        rc = liberasurecode_decode(desc, avail_frags, num_avail_frags,
                                   chunks->at(0)->size(), 1,
                                   &decoded_data, &decoded_data_len);
        if(rc != 0)
            DPRINTF(DEBUG_CAS_Client, "rc is %d.\n", rc);
        
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

}

CAS_Client::CAS_Client(uint32_t client_id, uint32_t local_datacenter_id, std::vector <DC*> &datacenters, int *desc_l, std::map<std::string, std::pair<uint32_t, Placement> > *keys_info) {
    
    assert(keys_info != nullptr);
    assert(desc_l != nullptr);
    
    this->local_datacenter_id = local_datacenter_id;
    retry_attempts = DEFAULT_RET_ATTEMPTS;
    metadata_server_timeout = DEFAULT_METASER_TO;
    timeout_per_request = 10000;
    this->keys_info = keys_info;
    
    
    // Todo: what is this???
    start_time = 0;
    
    this->datacenters = datacenters;
    
    this->current_class = CAS_PROTOCOL_NAME;
    this->id = client_id;
    this->desc = desc_l;
    this->desc_destroy = 1;

#ifdef LOGGING_ON
    char name[20];
    name[0] = 'l';
    name[1] = 'o';
    name[2] = 'g';
    name[3] = 's';
    name[4] = '/';

    sprintf(&name[5], "%u.txt", client_id);
    this->log_file = fopen(name, "w");
#endif

}

CAS_Client::~CAS_Client() {
    
#ifdef LOGGING_ON
    fclose(this->log_file);
#endif
    
    if(this->desc_destroy){
        if((*(this->desc)) != -1)
            destroy_liberasure_instance((*(this->desc)));
    }
    DPRINTF(DEBUG_CAS_Client, "cliend with id \"%u\" has been destructed.\n", this->id);
}

uint32_t CAS_Client::get_id(){
    return this->id;
}

void static inline free_chunks(std::vector<std::string*> &chunks){

    for(uint i = 0; i < chunks.size(); i++){
        delete chunks[i];
    }
    chunks.clear();
}

int CAS_Client::update_placement(const std::string &key, const uint32_t conf_id){
    int ret = 0;

    uint32_t requested_conf_id;
    uint32_t new_conf_id; // Not usefull for client
    std::string timestamp; // Not usefull for client
    Placement* p;

    ret = ask_metadata(key, conf_id, requested_conf_id, new_conf_id, timestamp, p, this->retry_attempts, this->metadata_server_timeout);

    assert(ret == 0);

    (*keys_info)[key] = std::pair<uint32_t, Placement>(requested_conf_id, *p);
    if(p->protocol == CAS_PROTOCOL_NAME){
        if((*(this->desc)) != -1)
            destroy_liberasure_instance((*(this->desc)));
        (*(this->desc)) = create_liberasure_instance(p);
    }
    ret = 0;

    delete p;
    p = nullptr;

//    auto it = keys_info->find(key);
//    if(it == keys_info->end()){
//        (*keys_info)[key] = std::pair<uint32_t, Placement>(requested_conf_id, *p);
//        if(p->protocol == CAS_PROTOCOL_NAME){
//            if((*(this->desc)) != -1)
//                destroy_liberasure_instance((*(this->desc)));
//            (*(this->desc)) = create_liberasure_instance(p);
//        }
//        ret = 0;
//    }
//    else{
//        uint32_t saved_conf_id = it->second.first;
//        if(status == "OK" && std::to_string(saved_conf_id) != std::to_string(conf_id)){
//
//            (*keys_info)[key] = std::pair<uint32_t, Placement>(requested_conf_id, *p);
//            ret =  0;
//            if(p->protocol == CAS_PROTOCOL_NAME){
//                if((*(this->desc)) != -1)
//                    destroy_liberasure_instance((*(this->desc)));
//                (*(this->desc)) = create_liberasure_instance(p);
//            }
//        }
//        else{
//            DPRINTF(DEBUG_CAS_Client, "msg is %s\n", msg.c_str());
//            ret = -1;
//        }
//
//        if(ret == 0 && (*(this->desc)) == -1){
//            (*(this->desc)) = create_liberasure_instance(p);
//        }
//
//        // Todo::: Maybe causes problem
//        delete p;
//        p = nullptr;
//
//        return ret;
//    }
    
    return ret;
}

Placement* CAS_Client::get_placement(std::string &key, bool force_update, uint32_t conf_id){ // Todo: a bug here is not to check that the requested confid is the same as key_info has.
    
    if(force_update){
        assert(update_placement(key, conf_id) == 0);
        auto it = this->keys_info->find(key);
        return &(it->second.second);
    }
    else{
        auto it = this->keys_info->find(key);
        if(it != this->keys_info->end()){
            return &(it->second.second);
        }
        else{
            assert(update_placement(key, 0) == 0);
            return &((*(this->keys_info))[key].second);
        }
    }
}

int CAS_Client::get_timestamp(std::string *key, Timestamp **timestamp, Placement **p){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

    uint32_t RAs = this->retry_attempts;
    
    std::vector<Timestamp> tss;
    *timestamp = nullptr;
    std::vector<std::future<strVec> > responses;
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
    
    
    responses.clear();
    tss.clear();
    RAs--;
    for(auto it = (*p)->Q1.begin();it != (*p)->Q1.end(); it++){

        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(CAS_thread_helper::_get_timestamp, std::move(prm), *key,
                        datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[*key].first).detach();
    }
    
    std::chrono::system_clock::time_point end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto &it:responses){
        if(it.wait_until(end) == std::future_status::timeout){
            // Access all the servers and wait for Q1.size() of them.
            op_status = -1; // You should access all the server.
            break;
        }
        
        strVec data = it.get();
        
        if(data.size() == 0){
            op_status = -1; // You should access all the server.
            break;
        }
        
        if(data[0] == "OK"){
            tss.emplace_back(data[1]);

            // Todo: make sure that the statement below is ture!
            op_status = 0;   // For get_timestamp, even if one response Received operation is success
        }else if(data[0] == "operation_fail"){
            DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key->c_str());
            *p = get_placement(*key, true, stoul(data[1]));
            assert(*p != nullptr);
            op_status = -2; // reconfiguration happened on the key
            *timestamp = nullptr;
            return S_RECFG;
            //break;
        }
        else{
            assert(false);
        }
        //else : The server returned "Failed", that means the entry was not found
        // We ignore the response in that case.
        // The servers can return Failed for timestamp, which is acceptable
    }
    
    uint32_t counter = ~((uint32_t)0);
    uint32_t num_fail_servers = 0;
    std::unordered_set<uint32_t> servers;

    set_intersection(*p, servers);
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        tss.clear();
        counter = 0;
        num_fail_servers = 0;
//        op_status = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(CAS_thread_helper::_get_timestamp, std::move(prm), *key,
                            datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[*key].first).detach();
        }

        std::chrono::system_clock::time_point end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
        
        for(uint i = 0; i < responses.size(); i++){ // Todo: need optimization to just take the fastes responses
            
            auto& it = responses[i];
            
            if(it.wait_until(end) == std::future_status::timeout){
                // Access all the servers and wait for Q1.size() of them.
                op_status = -1; // You should access all the server.
                break;
            }

            strVec data = it.get();
            
            if(data.size() == 0){
                num_fail_servers++;
                if(num_fail_servers > (*p)->f){
                    op_status = -100; // You should access all the server.
                    break;
                }
                else{
                    continue;
                }
            }

            if(data[0] == "OK"){
                tss.emplace_back(data[1]);

                // Todo: make sure that the statement below is ture!
                op_status = 0;   // For get_timestamp, even if one response Received operation is success
                counter++;
            }else if(data[0] == "operation_fail"){
                DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key->c_str());
                *p = get_placement(*key, true, stoul(data[1]));
                assert(*p != nullptr);
                op_status = -2; // reconfiguration happened on the key
                *timestamp = nullptr;
                return S_RECFG;
                //break;
            }
            else{
//                counter++;
                assert(false);
            }
            //else : The server returned "Failed", that means the entry was not found
            // We ignore the response in that case.
            // The servers can return Failed for timestamp, which is acceptable
            
            if(counter >= (*p)->Q1.size())
                break;
            
        }
    }
    
//    if(counter < (*p)->Q1.size()){
//        op_status = -100;
//        DPRINTF(DEBUG_CAS_Client, "More than f servers did not respond.\n");
//    }

    if(op_status == 0){
        *timestamp = new Timestamp(Timestamp::max_timestamp2(tss));
        
        DPRINTF(DEBUG_CAS_Client, "finished successfully. Max timestamp received is %s\n", (*timestamp)->get_string().c_str());
    }else{
        DPRINTF(DEBUG_CAS_Client, "Operation Failed. op_status is %d\n", op_status);
        assert(false);
        return S_FAIL;
    }

    return op_status;
}

int CAS_Client::put(std::string key, std::string value, bool insert){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

#ifdef LOGGING_ON
    gettimeofday (&log_tv, NULL);
    log_tm = localtime (&log_tv.tv_sec);
    strftime (log_fmt, sizeof (log_fmt), "%H:%M:%S:%%06u", log_tm);
    snprintf (log_buf, sizeof (log_buf), log_fmt, log_tv.tv_usec);
    fprintf(this->log_file, "%s write invoke %s\n", log_buf, value.c_str());
#endif
    
//    key = std::string("CAS" + key);
    
    Placement *p = get_placement(key);
    uint32_t RAs = this->retry_attempts;
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
    
    std::vector <std::string*> chunks;
    struct ec_args null_args;
    null_args.k = p->k;
    null_args.m = p->m - p->k; // m here is the number of parity chunks
    null_args.w = 16;
    null_args.ct = CHKSUM_NONE;

    DPRINTF(DEBUG_CAS_Client, "The value to be stored is %s \n", value.c_str());

    if((*(this->desc)) == -1){
        DPRINTF(DEBUG_CAS_Client, "liberasure instance is not initialized.\n");
        return GENERAL_ERASURE_ERROR;
    }
    std::thread encoder(liberasure::encode, &value, &chunks, &null_args, (*(this->desc)));
    //encode(&value, &chunks, &null_args, this->desc);

    Timestamp *timestamp = nullptr;
    Timestamp *tmp = nullptr;
    int status = S_OK;
    std::vector<std::future<strVec> > responses;

    if(insert){ // This is a new key
        timestamp = new Timestamp(this->id);
    }
    else{
        status = this->get_timestamp(&key, &tmp, &p);

        if(tmp != nullptr){
            timestamp = new Timestamp(tmp->increase_timestamp(this->id));
            delete tmp;
            tmp = nullptr;
        }
    }

    // Join the encoder thread
    encoder.join();

    if(timestamp == nullptr){
        free_chunks(chunks);

        if(status == S_RECFG){
            op_status = -2;
            return S_RECFG;
        }

        DPRINTF(DEBUG_CAS_Client, "get_timestamp operation failed key %s \n", key.c_str());
        
    }
    
    // prewrite
//    int i = 0;
//    char bbuf[1024*128];
//    int bbuf_i = 0;
//    bbuf_i += sprintf(bbuf + bbuf_i, "%s-main value is %s\n", key.c_str(), value.c_str());
//    for(uint t = 0; t < chunks.size(); t++){
//        bbuf_i += sprintf(bbuf + bbuf_i, "%s-chunk[%d] = ", key.c_str(), t);
//        for(uint tt = 0; tt < chunks[t]->size(); tt++){
//            bbuf_i += sprintf(bbuf + bbuf_i, "%02X", chunks[t]->at(tt) & 0xff);
//            printf("%02X", chunks[t]->at(tt));
//        }
//        bbuf_i += sprintf(bbuf + bbuf_i, "\n");
//    }
//    printf("%s", bbuf);

    responses.clear();
    RAs--;
    for(auto it = p->Q2.begin(); it != p->Q2.end(); it++){
        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(CAS_thread_helper::_put, std::move(prm), key, *chunks[*it], *timestamp,
                            datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();

        DPRINTF(DEBUG_CAS_Client, "Issue _put request to key: %s and timestamp: %s and conf_id: %u and chunk_size :%lu \n", key.c_str(), timestamp->get_string().c_str(), (*(this->keys_info))[key].first, chunks[*it]->size());
        // for (auto& el : *chunks[i])
        //      printf("%02hhx", el);
        // std::cout << '\n';
//        i++;
    }
    
    std::chrono::system_clock::time_point end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto &it:responses){
        
        if(it.wait_until(end) == std::future_status::timeout){
            // Access all the servers and wait for Q1.size() of them.
            op_status = -1; // You should access all the server.
            break;
        }
        
        strVec data = it.get();
        
        if(data.size() == 0){
            op_status = -1; // You should access all the server.
            break;
        }

        if(data[0] == "OK"){
            DPRINTF(DEBUG_CAS_Client, "OK received for key : %s\n", key.c_str());
        }
        else if(data[0] == "operation_fail"){
            DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
            p = get_placement(key, true, stoul(data[1]));
            assert(p != nullptr);
            op_status = -2; // reconfiguration happened on the key
            free_chunks(chunks);
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            return S_RECFG;
        }
        else{
            DPRINTF(DEBUG_CAS_Client, "Bad message received from server for key : %s\n", key.c_str());
            free_chunks(chunks);
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            return -3; // Bad message received from server
        }
    }
    
    uint32_t counter = ~((uint32_t)0);
    uint32_t num_fail_servers;
    std::unordered_set<uint32_t> servers;

    set_intersection(p, servers);
    
    while(op_status == -1 && RAs--){
//        DPRINTF(DEBUG_CAS_Client, "the whole number of servers: %lu\n", servers.size());
        responses.clear(); // ignore responses to old requests
        counter = 0;
        num_fail_servers = 0;
        op_status = 0;
        
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(CAS_thread_helper::_put, std::move(prm), key, *chunks[*it], *timestamp,
                            datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();
            DPRINTF(DEBUG_CAS_Client, "Issue _put request to key: %s and timestamp: %s and conf_id: %u and chunk_size :%lu \n", key.c_str(), timestamp->get_string().c_str(), (*(this->keys_info))[key].first, chunks[*it]->size());
//            i++;
        }
        
        std::chrono::system_clock::time_point end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
        for(uint i = 0; i < responses.size(); i++){ // Todo: need optimization to just take the fastes responses
            
            auto& it = responses[i];

            if(it.wait_until(end) == std::future_status::timeout){
                // Access all the servers and wait for Q1.size() of them.
                op_status = -1; // You should access all the server.
                break;
            }

            strVec data = it.get();
            
            if(data.size() == 0){
                num_fail_servers++;
                if(num_fail_servers > p->f){
                    op_status = -100;
                    break;
                }
                else{
                    continue;
                }
            }

            if(data[0] == "OK"){
                counter++;
            }
            else if(data[0] == "operation_fail"){
                DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
                p = get_placement(key, true, stoul(data[1]));
                assert(p != nullptr);
                op_status = -2; // reconfiguration happened on the key
                free_chunks(chunks);
                if(timestamp != nullptr){
                    delete timestamp;
                    timestamp = nullptr;
                }
                return S_RECFG;
            }
            else{
                DPRINTF(DEBUG_CAS_Client, "Bad message received from server for key : %s\n", key.c_str());
                free_chunks(chunks);
                if(timestamp != nullptr){
                    delete timestamp;
                    timestamp = nullptr;
                }
                return -3; // Bad message received from server
            }
            
            if(counter >= p->Q2.size()){
                op_status = 0;
                break;
            }
        }
    }
    
//    if(counter < p->Q2.size()){
//        op_status = -100;
//    }

    responses.clear();
    free_chunks(chunks);
    if(op_status != 0){
        DPRINTF(DEBUG_CAS_Client, "pre_write could not succeed. op_status is %d\n", op_status);
        if(timestamp != nullptr){
            delete timestamp;
            timestamp = nullptr;
        }
        return -4; // pre_write could not succeed.
    }
    
    
    // Fin
    RAs = this->retry_attempts;
    
    RAs--;
    for(auto it = p->Q3.begin(); it != p->Q3.end(); it++){
        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(CAS_thread_helper::_put_fin, std::move(prm), key, *timestamp,
                        datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();
        DPRINTF(DEBUG_CAS_Client, "Issue Q3 request to key: %s \n", key.c_str());
    }
    
    end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto &it:responses){
        
        if(it.wait_until(end) == std::future_status::timeout){
            // Access all the servers and wait for Q1.size() of them.
            op_status = -1; // You should access all the server.
            break;
        }
        
        strVec data = it.get();
        
        if(data.size() == 0){
            op_status = -1; // You should access all the server.
            break;
        }
        
        if(data[0] == "OK"){

        }
        else if(data[0] == "operation_fail"){
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
            p = get_placement(key, true, stoul(data[1]));
            assert(p != nullptr);
            op_status = -2; // reconfiguration happened on the key
            free_chunks(chunks);
            return S_RECFG;
        }
        else{
            op_status = -5; // Bad message received from server
            DPRINTF(DEBUG_CAS_Client, "Bad message received from server received for key : %s\n", key.c_str());
            free_chunks(chunks);
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            return -5;
        }
        
    }
    
    counter = ~((uint32_t)0);
//    std::unordered_set<uint32_t> servers;

//    set_intersection(p, servers);
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        counter = 0;
        num_fail_servers = 0;
        op_status = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers
            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(CAS_thread_helper::_put_fin, std::move(prm), key, *timestamp,
                            datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();
            DPRINTF(DEBUG_CAS_Client, "Issue Q3 request to key: %s \n", key.c_str());
        }
        
        std::chrono::system_clock::time_point end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
        for(uint i = 0; i < responses.size(); i++){ // Todo: need optimization to just take the fastes responses
            
            auto& it = responses[i];

            if(it.wait_until(end) == std::future_status::timeout){
                // Access all the servers and wait for Q1.size() of them.
                op_status = -1; // You should access all the server.
                break;
            }

            strVec data = it.get();
            
            if(data.size() == 0){
                num_fail_servers++;
                if(num_fail_servers > p->f){
                    op_status = -100;
                    break;
                }
                else{
                    continue;
                }
            }
            
            if(data[0] == "OK"){
                counter++;
            }
            else if(data[0] == "operation_fail"){
                if(timestamp != nullptr){
                    delete timestamp;
                    timestamp = nullptr;
                }
                DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
                p = get_placement(key, true, stoul(data[1]));
                assert(p != nullptr);
                op_status = -2; // reconfiguration happened on the key
                free_chunks(chunks);
                return S_RECFG;
            }
            else{
                op_status = -5; // Bad message received from server
                DPRINTF(DEBUG_CAS_Client, "Bad message received from server received for key : %s\n", key.c_str());
                free_chunks(chunks);
                if(timestamp != nullptr){
                    delete timestamp;
                    timestamp = nullptr;
                }
                return -5;
            }
            
            if(counter >= p->Q3.size()){
                op_status = 0;
                break;
            }
        }
    }
    
//    if(counter < p->Q3.size()){
//        op_status = -100;
//    }

    responses.clear();
    delete timestamp;
    if(op_status != 0){
        DPRINTF(DEBUG_CAS_Client, "Fin_write could not succeed. op_status is %d\n", op_status);
//        delete timestamp;
        return -6; // Fin_write could not succeed.
    }
    
    return op_status;
}

int CAS_Client::get(std::string key, std::string &value){
    
    DPRINTF(DEBUG_CAS_Client, "started.\n");
    
    bool uninitialized_key = false;

#ifdef LOGGING_ON
    gettimeofday (&log_tv, NULL);
    log_tm = localtime (&log_tv.tv_sec);
    strftime (log_fmt, sizeof (log_fmt), "%H:%M:%S:%%06u", log_tm);
    snprintf (log_buf, sizeof (log_buf), log_fmt, log_tv.tv_usec);
    fprintf(this->log_file, "%s read invoke nil\n", log_buf);
#endif
    value.clear();
    Placement *p = get_placement(key);
    uint32_t RAs = this->retry_attempts;
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

    Timestamp *timestamp = nullptr;
//    Timestamp *tmp = nullptr;
    int status = S_OK;
    std::vector<std::future<strVec> > responses;

    status = this->get_timestamp(&key, &timestamp, &p);
    
    if(status == S_RECFG){
        op_status = -2;
        return S_RECFG;
    }

    if(timestamp == nullptr){
        DPRINTF(DEBUG_CAS_Client, "get_timestamp operation failed key %s \n", key.c_str());
        assert(false);
    }

    
    // phase 2
    std::vector <std::string*> chunks;
    
    RAs--;
    for(auto it = p->Q4.begin(); it != p->Q4.end(); it++){
        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(CAS_thread_helper::_get, std::move(prm), key, *timestamp,
                datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();

        DPRINTF(DEBUG_CAS_Client, "Issue Q4 request for key :%s \n", key.c_str());
    }
    
    std::chrono::system_clock::time_point end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto &it:responses){
        
        if(it.wait_until(end) == std::future_status::timeout){
            // Access all the servers and wait for Q1.size() of them.
            op_status = -1; // You should access all the server.
            break;
        }
        
        strVec data = it.get();
        
        if(data.size() == 0){
            op_status = -1; // You should access all the server.
            break;
        }
        
        if(data[0] == "OK"){
            if(data[1] == "Ack"){
                
            }
            else if(data[1] == "init"){
                uninitialized_key = true;
            }
            else{
                chunks.push_back(new std::string(data[1]));
            }
        }else if(data[0] == "operation_fail"){
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
            p = get_placement(key, true, stoul(data[1]));
            assert(p != nullptr);
            op_status = -2; // reconfiguration happened on the key
            free_chunks(chunks);
            return S_RECFG;
        }
        else{
            assert(false);
            // Todo: it is possible and fine that a server in Q4 doesn't have the chunk.
//            op_status = -8; // Bad message received from server
//            DPRINTF(DEBUG_CAS_Client, "Bad message received from server received for key : %s\n", key.c_str());
//            free_chunks(chunks);
//            delete timestamp;
//            return -8;
        }
        
//        if(uninitialized_key){
//            break;
//        }
    }
    
    uint32_t counter = ~((uint32_t)0);
    uint32_t num_fail_servers;
    std::unordered_set<uint32_t> servers;

    set_intersection(p, servers);
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        counter = 0;
        free_chunks(chunks);
        op_status = 0;
        uninitialized_key = false;
        num_fail_servers = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers
            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(CAS_thread_helper::_get, std::move(prm), key, *timestamp,
                    datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();

            DPRINTF(DEBUG_CAS_Client, "Issue Q4 request for key :%s \n", key.c_str());
        }
        
        std::chrono::system_clock::time_point end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
        for(uint i = 0; i < responses.size(); i++){ // Todo: need optimization to just take the fastes responses
            
            auto& it = responses[i];

            if(it.wait_until(end) == std::future_status::timeout){
                // Access all the servers and wait for Q1.size() of them.
                op_status = -1; // You should access all the server.
                std::cerr << "AAAAAA" << std::endl;
                break;
            }

            strVec data = it.get();
            
            if(data.size() == 0){
                num_fail_servers++;
                if(num_fail_servers > p->f){
                    op_status = -100;
                    break;
                }
                else{
                    continue;
                }
            }
            
            if(data[0] == "OK"){
                counter++;
                if(data[1] == "Ack"){
                }
                else if(data[1] == "init"){
                    uninitialized_key = true;
                }
                else{
                    chunks.push_back(new std::string(data[1]));
                    
//                    printf("aaaaaaaaaaaa: ");
//                    for(int i = 0; i < data[1].size(); i++){
//                        printf("%c", data[1][i]);
//                    }
//                    printf("\n");
                    
                }
            }else if(data[0] == "operation_fail"){
                if(timestamp != nullptr){
                    delete timestamp;
                    timestamp = nullptr;
                }
                DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
                p = get_placement(key, true, stoul(data[1]));
                assert(p != nullptr);
                op_status = -2; // reconfiguration happened on the key
                free_chunks(chunks);
                return S_RECFG;
            }
            else{ // Todo: it is possible and fine that a server in Q4 doesn't have the chunk.
//                counter++;
                assert(false);
//                op_status = -8; // Bad message received from server
//                DPRINTF(DEBUG_CAS_Client, "Bad message received from server received for key : %s\n", key.c_str());
//                free_chunks(chunks);
//                delete timestamp;
//                return -8;
            }
            
            if(counter >= p->Q4.size() && chunks.size() >= p->k){
                op_status = 0;
                break;
            }
        }
    }
    
//    if(!(counter >= p->Q4.size() && chunks.size() >= p->k)){
//        op_status = -100;
//    }

    responses.clear();
    if(timestamp != nullptr){
        delete timestamp;
        timestamp = nullptr;
    }
    
    if(!uninitialized_key){
        if(chunks.size() < p->k){
            free_chunks(chunks);
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            op_status = -9;
            DPRINTF(DEBUG_CAS_Client, "chunks.size() < p->k key : %s\n", key.c_str());
        }

        if(op_status != 0){
            DPRINTF(DEBUG_CAS_Client, "get operation failed for key: %s op_status is %d\n", key.c_str(), op_status);
            free_chunks(chunks);
            return -10;
        }

        // Decode only if above operation success
        struct ec_args null_args;
        null_args.k = p->k;
        null_args.m = p->m - p->k;
        null_args.w = 16; // ToDo: what must it be?
        null_args.ct = CHKSUM_NONE;

    //    char bbuf[1024*128];
    //    int bbuf_i = 0;
    //    bbuf_i += sprintf(bbuf + bbuf_i, "%s-get function value is %s\n", key.c_str(), value.c_str());
    //    for(uint t = 0; t < chunks.size(); t++){
    //        bbuf_i += sprintf(bbuf + bbuf_i, "%s-chunk[%d] = ", key.c_str(), t);
    //        for(uint tt = 0; tt < chunks[t]->size(); tt++){
    //            bbuf_i += sprintf(bbuf + bbuf_i, "%02X", chunks[t]->at(tt) & 0xff);
    ////                printf("%02X", chunks[t]->at(tt));
    //        }
    //        bbuf_i += sprintf(bbuf + bbuf_i, "\n");
    //    }
    //    printf("%s", bbuf);

        liberasure::decode(&value, &chunks, &null_args, (*(this->desc)));
    }
    else{
        if(op_status != 0){
            DPRINTF(DEBUG_CAS_Client, "get operation failed for key: %s op_status is %d\n", key.c_str(), op_status);
            free_chunks(chunks);
            return -10;
        }
        else{
            value = "__Uninitiliazed";
        }
    }
    
    free_chunks(chunks);

    return op_status;
}
