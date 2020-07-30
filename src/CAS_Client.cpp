/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   CAS_Client.cpp
 * Author: Praneet Soni
 *
 * Created on January 4, 2020, 11:35 PM
 */

#include "CAS_Client.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <regex>
#include <future>
#include <sys/time.h>

char fmt_cas[64];
char buf_cas[64];
struct timeval tv_cas;
struct tm *tm22_cas;

//TODO: Ask more servers for timestamp if last attempt didn't work.

// Use this constructor when single threaded code
CAS_Client::CAS_Client(Properties *prop, uint32_t client_id, Placement &pp) {
    this->current_class = "CAS";
    this->id = client_id;
    this->prop = prop;
    this->p = pp;
    this->desc = create_liberasure_instance(&(this->p));
    this->desc_destroy = 1;
}

CAS_Client::CAS_Client(Properties *prop, uint32_t client_id, Placement &pp, int desc_l) {
    this->current_class = "CAS";
    this->id = client_id;
    this->prop = prop;
    this->p = pp;
    this->desc = desc_l;
    this->desc_destroy = 0;

    // char name[20];
    // this->operation_id = 0;
    // name[0] = 'l';
    // name[1] = 'o';
    // name[2] = 'g';
    // name[3] = 's';
    // name[4] = '/';

    //sprintf(&name[5], "%u.txt", client_id);
    //this->log_file = fopen(name, "w");

}


CAS_Client::~CAS_Client() {
    //fclose(this->log_file);
    if(this->desc_destroy){
        destroy_liberasure_instance(this->desc);
    }
    DPRINTF(DEBUG_CAS_Client, "cliend with id \"%u\" has been destructed.\n", this->id);
}

void CAS_Client::update_placement(std::string &new_cfg){

    this->p = DataTransfer::deserializeCFG(new_cfg);
    if(this->desc_destroy){
        destroy_liberasure_instance(this->desc);
        this->desc = create_liberasure_instance(&(this->p));
    }
}

void _get_timestamp(std::promise<strVec> &&prm, std::string key, Server *server,
                    std::string current_class){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

    int sock = 0;
    if(client_cnt(sock, server) == S_FAIL){
        return;
    }
    
    strVec data;
    data.push_back("get_timestamp");
    data.push_back(key);
    data.push_back(current_class);
    DataTransfer::sendMsg(sock, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;

    // If the socket recv itself fails, then 'promise' value will not be made available
    if(DataTransfer::recvMsg(sock, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        prm.set_value(std::move(data));
    }

    close(sock);
    return;
}

uint32_t CAS_Client::get_timestamp(std::string *key, Timestamp **timestamp){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

    std::vector<Timestamp> tss;
    *timestamp = nullptr;
    std::vector<std::future<strVec> > responses;
    int retries = this->prop->retry_attempts;
    bool op_status = false;    //Success

    while(!op_status && retries--){
        for(auto it = p.Q1.begin();it != p.Q1.end(); it++){

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(_get_timestamp, std::move(prm), *key,
                            prop->datacenters[*it]->servers[0], this->current_class).detach();
        }

        for(auto &it:responses){
            strVec data = it.get();

            if(data[0] == "OK"){
                printf("get timestamp for key :%s, data received is %s\n", key->c_str(), data[1].c_str());
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
                op_status = true;   // For get_timestamp, even if one response Received
                                    // operation is success
            }else if(data[0] == "operation_fail"){
                update_placement(data[1]);
                op_status = false;      // Retry the operation
                printf("get_timestamp : operation failed received! for key : %s", key->c_str());
                return S_RECFG;
                //break;
            }
            //else : The server returned "Failed", that means the entry was not found
            // We ignore the response in that case.
            // The servers can return Failed for timestamp, which is acceptable
        }

        responses.clear();
    }

    if(op_status){
        *timestamp = new Timestamp(Timestamp::max_timestamp2(tss));
        DPRINTF(DEBUG_CAS_Client, "finished successfully.\n");
    }else{
        DPRINTF(DEBUG_CAS_Client, "Operation Failed.\n");
        return S_FAIL;
    }

    return S_OK;
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

void _put(std::promise<strVec> &&prm, std::string key, std::string value, Timestamp timestamp,
                    Server *server, std::string current_class){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

    int sock = 0;
    if(client_cnt(sock, server) == S_FAIL){
        return;
    }

    strVec data;
    data.push_back("put");

    data.push_back(key);
    data.push_back(timestamp.get_string());
    data.push_back(value);
    data.push_back(current_class);

    if((value).empty()){
	       printf("WARNING!!! SENDING EMPTY STRING TO SERVER\n");
    }
    DataTransfer::sendMsg(sock,DataTransfer::serialize(data));

    data.clear();
    std::string recvd;


    // If the socket recv itself fails, then 'promise' value will not be made available
    if(DataTransfer::recvMsg(sock, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        prm.set_value(std::move(data));
    }

    DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server->port);
    close(sock);

    return;
}

void _put_fin(std::promise<strVec> &&prm, std::string key, Timestamp timestamp,
                    Server *server, std::string current_class){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

    int sock = 0;
    if(client_cnt(sock, server) == S_FAIL){
        return;
    }

    strVec data;
    data.push_back("put_fin");
    data.push_back(key);
    data.push_back(timestamp.get_string());
    data.push_back(current_class);
    DataTransfer::sendMsg(sock, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;

    // If the socket recv itself fails, then 'promise' value will not be made available
    if(DataTransfer::recvMsg(sock, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        prm.set_value(std::move(data));
    }

    DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server->port);
    close(sock);

    return;
}

void static inline free_chunks(std::vector<std::string*> &chunks){

    for(uint i = 0; i < chunks.size(); i++){
        delete chunks[i];
    }
}


uint32_t CAS_Client::put(std::string key, std::string value, bool insert){

//    gettimeofday (&tv_cas, NULL);
//    tm22_cas = localtime (&tv_cas.tv_sec);
//    strftime (fmt_cas, sizeof (fmt_cas), "%H:%M:%S:%%06u", tm22_cas);
//    snprintf (buf_cas, sizeof (buf_cas), fmt_cas, tv_cas.tv_usec);
//    fprintf(this->log_file, "%s write invoke %s\n", buf_cas, value.c_str());


    int retries = this->prop->retry_attempts;
    bool op_status = false;

    while(!op_status && retries--){

        std::vector <std::string*> chunks;
        op_status = true;
        struct ec_args null_args;
        null_args.k = p.k;
        null_args.m = p.m - p.k;
        null_args.w = 16;
        null_args.ct = CHKSUM_NONE;

        DPRINTF(DEBUG_CAS_Client, "The value to be stored is %s \n", value.c_str());

        std::thread encoder(encode, &value, &chunks, &null_args, this->desc);
        //encode(&value, &chunks, &null_args, this->desc);

        Timestamp *timestamp = nullptr;
        Timestamp *tmp = nullptr;
        int status = S_OK;
        std::vector<std::future<strVec> > responses;

        if(insert){ // This is a new key
            timestamp = new Timestamp(this->id);
        }
        else{
            status = this->get_timestamp(&key, &tmp);

            if(tmp != nullptr){
                timestamp = new Timestamp(tmp->increase_timestamp(this->id));
                delete tmp;
            }
        }

        // Join the encoder thread
        encoder.join();

        if(timestamp == nullptr){
            free_chunks(chunks);

            // Temporary fix for reconfig protocol, because
            // EC library not thread safe, so cannot create new 'desc'
            if(status == S_RECFG && !this->desc_destroy){
                return S_RECFG;
            }

            DPRINTF(DEBUG_CAS_Client, "get_timestamp operation failed key %s \n", key.c_str());
            op_status = false;
            continue;
        }


        // prewrite
        int i = 0;

        for(auto it = p.Q2.begin(); it != p.Q2.end(); it++){
            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(_put, std::move(prm), key, *chunks[i], *timestamp,
                                prop->datacenters[*it]->servers[0], this->current_class).detach();

            DPRINTF(DEBUG_CAS_Client, "Issue Q2 request to key: %s and timestamp: %s  chunk_size :%lu  chunks: ", key.c_str(), timestamp->get_string().c_str(), chunks[i]->size());
            // for (auto& el : *chunks[i])
      	    //      printf("%02hhx", el);
            // std::cout << '\n';
            i++;
        }

        free_chunks(chunks);

        for(auto &it:responses){
            strVec data = it.get();

            if(data[0] == "OK"){

            } else{
                delete timestamp;
                if(data[0] == "operation_fail"){
                    update_placement(data[1]);

                    // Temporary fix for reconfig protocol, because
                    // EC library not thread safe, so cannot create new 'desc'
                    if(!this->desc_destroy){
                        return S_RECFG;
                    }
                }
                op_status = false;
                printf("_put operation failed for key : %s   %s \n", key.c_str(), data[0].c_str());
                break;
            }
        }

        responses.clear();

        if(!op_status){
            continue;
        }

        for(auto it = p.Q3.begin(); it != p.Q3.end(); it++){

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(_put_fin, std::move(prm), key, *timestamp,
                            prop->datacenters[*it]->servers[0], this->current_class).detach();
            DPRINTF(DEBUG_CAS_Client, "Issue Q3 request to key: %s \n", key.c_str());
        }

        delete timestamp;

        for(auto &it:responses){
            strVec data = it.get();

            if(data[0] == "OK"){

            }else{
                if(data[0] == "operation_fail"){
                    update_placement(data[1]);

                    // Temporary fix for reconfig protocol, because
                    // EC library not thread safe, so cannot create new 'desc'
                    if(!this->desc_destroy){
                        return S_RECFG;
                    }
                }
                op_status = false;
                printf("_put_fin operation failed for key :%s  %s \n", key.c_str(), data[0].c_str());
                break;
            }
        }

        responses.clear();

    }

    return op_status? S_OK : S_FAIL;
}

void _get(std::promise<strVec> &&prm, std::string key, Timestamp timestamp,
            Server *server, std::string current_class){

    DPRINTF(DEBUG_CAS_Client, "started.\n");

    int sock = 0;
    if(client_cnt(sock, server) == S_FAIL){
        return;
    }

    strVec data;
    data.push_back("get");
    data.push_back(key);
    data.push_back(timestamp.get_string());
    data.push_back(current_class);
    DataTransfer::sendMsg(sock, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;

    // If the socket recv itself fails, then 'promise' value will not be made available
    if(DataTransfer::recvMsg(sock, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        prm.set_value(std::move(data));
    }

    close(sock);
    DPRINTF(DEBUG_CAS_Client, "finished successfully.\n");

    return;
}

uint32_t CAS_Client::get(std::string key, std::string &value){

//    gettimeofday (&tv_cas, NULL);
//    tm22_cas = localtime (&tv_cas.tv_sec);
//    strftime (fmt_cas, sizeof (fmt_cas), "%H:%M:%S:%%06u", tm22_cas);
//    snprintf (buf_cas, sizeof (buf_cas), fmt_cas, tv_cas.tv_usec);
//    fprintf(this->log_file, "%s read invoke nil\n", buf_cas);


    value.clear();

    int retries = this->prop->retry_attempts;
    bool op_status = false;

    while(!op_status && retries--){

        Timestamp *timestamp = nullptr;
        int status = this->get_timestamp(&key, &timestamp);
        op_status = true;

        // Temporary fix for reconfig protocol, because
        // EC library not thread safe, so cannot create new 'desc'
        if(status == S_RECFG && !this->desc_destroy){
            return S_RECFG;
        }

        if(timestamp == nullptr){
            DPRINTF(DEBUG_CAS_Client, "get_timestamp operation failed %s \n", value.c_str());
            op_status = false;
            continue;
        }

        // phase 2
        std::vector <std::string*> chunks;
        std::vector <std::future<strVec> > responses;

        for(auto it = p.Q4.begin(); it != p.Q4.end(); it++){
            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(_get, std::move(prm), key, *timestamp,
                    prop->datacenters[*it]->servers[0], this->current_class).detach();

            DPRINTF(DEBUG_CAS_Client, "Issue Q4 request for key :%s \n", key.c_str());
        }

        delete timestamp;

        for(auto &it:responses){
            strVec data = it.get();

            if(data[0] == "OK"){
                chunks.push_back(new std::string(data[1]));
            }else{
                free_chunks(chunks);
                if(data[0] == "operation_fail"){
                    update_placement(data[1]);

                    // Temporary fix for reconfig protocol, because
                    // EC library not thread safe, so cannot create new 'desc'
                    if(!this->desc_destroy){
                        return S_RECFG;
                    }
                }
                op_status = false;
                printf("get operation failed for key:%s  %s\n", key.c_str(), data[0].c_str());
                break;
            }
        }

        responses.clear();

        if(!op_status){
            continue;
        }
        // Decode only if above operation success
        struct ec_args null_args;
        null_args.k = p.k;
        null_args.m = p.m - p.k;
        null_args.w = 16; // ToDo: what must it be?
        null_args.ct = CHKSUM_NONE;

        decode(&value, &chunks, &null_args, this->desc);

        free_chunks(chunks);
    }
    return op_status? S_OK: S_FAIL;
}
