/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   Reconfig.cpp
 * Author: Praneet Soni
 *
 * Created on March 23, 2020, 6:34 PM
 */

#include "Reconfig.h"
#include <algorithm>
#include <future>
#include <unordered_set>

using std::string;

template <typename T>
void set_intersection(Placement *p, std::unordered_set<T> &res){
    res.insert(p->Q1.begin(), p->Q1.end());
    res.insert(p->Q2.begin(), p->Q2.end());
    res.insert(p->Q3.begin(), p->Q3.end());
    res.insert(p->Q4.begin(), p->Q4.end());
}

int Reconfig::get_metadata_info(std::string &key, GroupConfig **old_config){
	std::unique_lock<std::mutex> lock(lock_t);
	if(key_metadata.find(key) == key_metadata.end()){
		//Key not found
		*old_config = nullptr;
		return 1;
	}else{
		*old_config = key_metadata[key];
		return 0;
	}
}

int Reconfig::update_metadata_info(std::string &key, GroupConfig *new_config){
	std::unique_lock<std::mutex> lock(lock_t);
	key_metadata[key] = new_config;
	return 0;
}

int Reconfig::start_reconfig(Properties *prop, GroupConfig &old_config, GroupConfig &new_config,
                                                            std::string key, int old_desc, int new_desc){

    Timestamp *ret_ts = nullptr;
    std::string ret_v;
//    key = std::string(old_config.placement_p->protocol + key);
    Reconfig::send_reconfig_query(prop, old_config, key, &ret_ts, ret_v);
    if(old_config.placement_p->protocol == CAS_PROTOCOL_NAME){
        Reconfig::send_reconfig_finalize(prop, old_config, key, ret_ts, ret_v, old_desc);
    }
    Reconfig::send_reconfig_write(prop, new_config, key, ret_ts, ret_v, new_desc);

    update_metadata_info(key, &new_config);

    Reconfig::send_reconfig_finish(prop, old_config, new_config, key, ret_ts);

    delete ret_ts;
    return S_OK;
}

void _send_reconfig_query(std::promise<strVec> &&prm, std::string key, Server *server,
                            std::string current_class){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\" and port %hu\n", key.c_str(), server->port);

    Connect c(server->ip, server->port);
    if(!c.is_connected()){
        return;
    }

    strVec data;
    data.push_back("reconfig_query");
    data.push_back(key);
    data.push_back(current_class);
    DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;

    if(DataTransfer::recvMsg(*c, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        DPRINTF(DEBUG_RECONFIG_CONTROL, "recieved data for key \"%s\" and port %hu: data is %s\n", key.c_str(), server->port, data[1].c_str());
        prm.set_value(std::move(data));
    }

    DPRINTF(DEBUG_RECONFIG_CONTROL, "finished on key \"%s\" and port %hu\n", key.c_str(), server->port);
    return;
}

int Reconfig::send_reconfig_query(Properties *prop, GroupConfig &old_config, std::string &key, Timestamp **ret_ts, std::string &ret_v){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());
    std::vector<Timestamp> tss;
    std::vector<string> vs;
    std::unordered_set<uint32_t> old_servers;
    std::vector<std::future<strVec> > responses;

    Placement *p = old_config.placement_p;
    set_intersection(p, old_servers);

    //TODO:: CHeck this and confirm
    //old_servers.insert()

    if(p->protocol == ABD_PROTOCOL_NAME){
        
        for(auto it = old_servers.begin(); it != old_servers.end(); it++){

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(_send_reconfig_query, std::move(prm), key,
                            prop->datacenters[*it]->servers[0], p->protocol).detach();
        }

        uint32_t max_val = (uint32_t)(old_servers.size() - p->Q2.size() + 1); // Todo: change it to old.Q1

        //POll all the values until you find the requisite responses
        // This is a busy loop, but it's on controller so it's fine
        while(max_val){
            for(auto &it:responses){
                if(it.valid() && it.wait_for(std::chrono::milliseconds(10)) == std::future_status::ready){

                    strVec data = it.get();

                    if(data[0] == "OK"){
                        DPRINTF(DEBUG_RECONFIG_CONTROL, "get timestamp for key :%s, ts: %s, value: %s\n", key.c_str(), data[2].c_str(), data[1].c_str());
                        vs.emplace_back(data[1]);
                        tss.emplace_back(data[2]);
                    }
                    else{
                        DPRINTF(DEBUG_RECONFIG_CONTROL, "reconfig query, ERROR in received data: %s\n", data[1].c_str());
                    }
                    //if "Failed", then need to retry
                    //reponses.erase(it);
                    max_val--;
                    break;
                }
            }

        }

        uint32_t max_ts_i = Timestamp::max_timestamp3(tss);
        *ret_ts = new Timestamp(tss[max_ts_i]);
        ret_v = vs[max_ts_i];
    }
    else if(p->protocol == CAS_PROTOCOL_NAME) {

        for(auto it = old_servers.begin(); it != old_servers.end(); it++){

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(_send_reconfig_query, std::move(prm), key,
                            prop->datacenters[*it]->servers[0], p->protocol).detach();
        }

//        uint32_t max_val = std::max(old_servers.size() - p->f, old_servers.size() - p->Q3.size() + 1);
        uint32_t max_val = std::max((uint32_t)(old_servers.size() - p->Q3.size() + 1), (uint32_t)(old_servers.size() - p->Q4.size() + 1)); // Todo: change it to old.Q1

        //POll all the values until you find the requisite responses
        // This is a busy loop, but it's on controller so it's fine
        while(max_val){
            for(auto &it:responses){
                if(it.valid() && it.wait_for(std::chrono::milliseconds(10)) == std::future_status::ready){

                    strVec data = it.get();

                    if(data[0] == "OK"){
                        printf("reconfig query, data received is %s\n", data[1].c_str());
                        tss.emplace_back(data[1]);
                    }
                    else{
                        printf("reconfig query, ERROR in received data: %s\n", data[1].c_str());
                    }
                    //if "Failed", then need to retry
                    //reponses.erase(it);
                    max_val--;
                    printf("max_val for key %s is %u\n", key.c_str(), max_val);
                    break;
                    
                    
                }
            }

        }

        *ret_ts = new Timestamp(Timestamp::max_timestamp2(tss));

    }

    DPRINTF(DEBUG_RECONFIG_CONTROL, "finished successfully on key \"%s\"\n", key.c_str());

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

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\" and port %hu\n", key.c_str(), server->port);

    Connect c(server->ip, server->port);
    if(!c.is_connected()){
        return;
    }

    strVec data;
    data.push_back("reconfig_finalize");
    data.push_back(key);
    data.push_back(timestamp.get_string());
    data.push_back(current_class);
    DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;

    if(DataTransfer::recvMsg(*c, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        prm.set_value(std::move(data));
    }
    
    DPRINTF(DEBUG_RECONFIG_CONTROL, "finished on key \"%s\" and port %hu\n", key.c_str(), server->port);
    
    return;
}

int Reconfig::send_reconfig_finalize(Properties *prop, GroupConfig &old_config, std::string &key,
                                Timestamp *ret_ts, std::string &ret_v, int desc){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());

    std::vector<std::future<strVec> > responses;
    std::vector <std::string*> chunks;

    Placement *p = old_config.placement_p;

    for(auto it = p->Q4.begin(); it != p->Q4.end(); it++){
        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(_send_reconfig_finalize, std::move(prm), key, *ret_ts,
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
        assert(0);
    }
    
//    for(int t = 0; t < chunks.size(); t++){
//        printf("chunk[%d] = ", t);
//        for(int tt = 0; tt < chunks[t]->size(); tt++){
//            printf("%02X", chunks[t]->at(tt));
//        }
//        printf("\n");
//    }
    
    char bbuf[1024*128];
        int bbuf_i = 0;
        for(uint t = 0; t < chunks.size(); t++){
            bbuf_i += sprintf(bbuf + bbuf_i, "%s-chunk[%d] = ", key.c_str(), t);
            for(uint tt = 0; tt < chunks[t]->size(); tt++){
                bbuf_i += sprintf(bbuf + bbuf_i, "%02X", chunks[t]->at(tt) & 0xff);
//                printf("%02X", chunks[t]->at(tt));
            }
            bbuf_i += sprintf(bbuf + bbuf_i, "\n");
        }
        printf("%s", bbuf);

    // Decode
    struct ec_args null_args;
    null_args.k = p->k;
    null_args.m = p->m - p->k;
    null_args.w = 16; // ToDo: what must it be?
    null_args.ct = CHKSUM_NONE;
    
    decode(&ret_v, &chunks, &null_args, desc);

    for(auto it: chunks){
        delete it;
    }

    DPRINTF(DEBUG_RECONFIG_CONTROL,"finished value for key \"%s\" is: \"%s\"\n", key.c_str(), ret_v.c_str());
    return S_OK;
}

void _send_reconfig_write(std::promise<strVec> &&prm, std::string key, Server *server,
                    Timestamp timestamp, std::string value, std::string current_class){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\" and port %hu\n", key.c_str(), server->port);

    Connect c(server->ip, server->port);
    if(!c.is_connected()){
        return;
    }

    strVec data;
    data.push_back("write_config");
    data.push_back(key);
    data.push_back(timestamp.get_string());
    data.push_back(value);
    data.push_back(current_class);

    DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;

    if(DataTransfer::recvMsg(*c, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        prm.set_value(std::move(data));
    }
    DPRINTF(DEBUG_RECONFIG_CONTROL, "finished on key \"%s\" and port %hu\n", key.c_str(), server->port);
    return;
}


int Reconfig::send_reconfig_write(Properties *prop, GroupConfig &new_config, std::string &key,
                        Timestamp *ret_ts, std::string &ret_v, int desc){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());
    Placement *p = new_config.placement_p;
    std::vector<std::future<strVec> > responses;
    std::vector <std::string*> chunks;

    struct ec_args null_args;
    null_args.k = p->k;
    null_args.m = p->m - p->k;
    null_args.w = 16;
    null_args.ct = CHKSUM_NONE;

    int i = 0;
    
//    key.erase(key.begin());
//    key.erase(key.begin());
//    key.erase(key.begin());
//    
//    key = std::string(p->protocol + key);

    if(p->protocol == ABD_PROTOCOL_NAME){

        for(auto it = p->Q2.begin(); it != p->Q2.end(); it++){

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(_send_reconfig_write, std::move(prm), key,
                    prop->datacenters[*it]->servers[0], *ret_ts, ret_v, p->protocol).detach();
            i++;
        }

        for(auto &it: responses){
            strVec data = it.get();

            if(data[0] == "OK"){}
            else{
                // failure! send the message to all servers.
            }
        }
    }
    else if(p->protocol == CAS_PROTOCOL_NAME){

        encode(&ret_v, &chunks, &null_args, desc);
        
        char bbuf[1024*128];
        int bbuf_i = 0;
        bbuf_i += sprintf(bbuf + bbuf_i, "%s-main ret_v is %s\n", key.c_str(), ret_v.c_str());
        for(uint t = 0; t < chunks.size(); t++){
            bbuf_i += sprintf(bbuf + bbuf_i, "%s-chunk[%d] = ", key.c_str(), t);
            for(uint tt = 0; tt < chunks[t]->size(); tt++){
                bbuf_i += sprintf(bbuf + bbuf_i, "%02X", chunks[t]->at(tt) & 0xff);
//                printf("%02X", chunks[t]->at(tt));
            }
            bbuf_i += sprintf(bbuf + bbuf_i, "\n");
        }
        printf("%s", bbuf);
        
        // Should be sent to union of Q2 and Q3
        std::unordered_set<uint32_t> Q2_Q3;
        Q2_Q3.insert(p->Q2.begin(), p->Q2.end());
        Q2_Q3.insert(p->Q3.begin(), p->Q3.end());

        for(auto it = Q2_Q3.begin(); it != Q2_Q3.end(); it++){

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(_send_reconfig_write, std::move(prm), key,
                    prop->datacenters[*it]->servers[0], *ret_ts, *chunks[i], p->protocol).detach();
            i++;
        }

        for(auto &it: responses){
            strVec data = it.get();

            if(data[0] == "OK"){}
            else{
                // failure! send the message to all servers.
            }
        }

        for(auto it: chunks){
            delete it;
        }
    }

    DPRINTF(DEBUG_RECONFIG_CONTROL, "finished successfully on key \"%s\"\n", key.c_str());

    return S_OK;
}

void _send_reconfig_finish(std::promise<strVec> &&prm, std::string key, Timestamp timestamp,
                    Server *server, std::string new_config){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\" and port %hu\n", key.c_str(), server->port);

    Connect c(server->ip, server->port);
    if(!c.is_connected()){
        return;
    }

    strVec data;
    data.push_back("finish_reconfig");
    data.push_back(key);
    data.push_back(timestamp.get_string());
    data.push_back(new_config);
    DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

    data.clear();
    std::string recvd;

    if(DataTransfer::recvMsg(*c, recvd) == 1){
        data =  DataTransfer::deserialize(recvd);
        prm.set_value(std::move(data));
    }
    DPRINTF(DEBUG_RECONFIG_CONTROL, "finished on key \"%s\" and port %hu\n", key.c_str(), server->port);
    return;
}

int Reconfig::send_reconfig_finish(Properties *prop, GroupConfig &old_config, GroupConfig &new_config,
                                        std::string &key, Timestamp *ret_ts){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());

    std::unordered_set<uint32_t> old_servers;
    std::vector<std::future<strVec> > responses;
    Placement *p = old_config.placement_p;
    std::string new_cfg = DataTransfer::serializeCFG(*new_config.placement_p);

    set_intersection(p, old_servers);

    for(auto it = old_servers.begin(); it != old_servers.end(); it++){
        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(_send_reconfig_finish, std::move(prm), key,
                    *ret_ts, prop->datacenters[*it]->servers[0], new_cfg).detach();
    }

    for(auto &it:responses){
        strVec data = it.get();
        if(data[0] == "OK"){
            // Finish reconfig reached the server
        }
        else{
            // failure! send the message to all servers.
        }
    }

    DPRINTF(DEBUG_RECONFIG_CONTROL,"finished on key \"%s\"\n", key.c_str());
    return S_OK;
}
