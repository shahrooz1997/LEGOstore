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
#include "Client_Node.h"
#include "../inc/CAS_Client.h"

using namespace std;

namespace CAS_helper{
    
    void _send_one_server(const string operation, promise<strVec>&& prm, const  string key,
            const Server server, const string current_class, const uint32_t conf_id, const string value = "",
            const string timestamp = ""){
        
        DPRINTF(DEBUG_CAS_Client, "started.\n");

        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with conf_id " + to_string(conf_id), DEBUG_CAS_Client);
    
        strVec data;
        Connect c(server.ip, server.port);
        if(!c.is_connected()){
            prm.set_value(move(data));
            return;
        }

        EASY_LOG_M(string("Connected to ") + server.ip + ":" + to_string(server.port));
    
        data.push_back(operation); // get_timestamp, put, put_fin, get
        data.push_back(key);
        if(operation == "put"){
            if(value.empty()){
                DPRINTF(DEBUG_CAS_Client, "WARNING!!! SENDING EMPTY STRING TO SERVER.\n");
            }
            data.push_back(timestamp);
            data.push_back(value);
        }
        else if(operation == "put_fin" || operation == "get"){
            data.push_back(timestamp);
        }
        else if(operation == "get_timestamp"){
        
        }
        else{
            assert(false);
        }

        data.push_back(current_class);
        data.push_back(to_string(conf_id));
        
        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        EASY_LOG_M("request sent");
    
        data.clear();
        string recvd;
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data = DataTransfer::deserialize(recvd);
            EASY_LOG_M(string("response received with status: ") + data[0]);
            prm.set_value(move(data));
        }
        else{
            data.clear();
            EASY_LOG_M("response failed");
            prm.set_value(move(data));
        }
    
        DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server.port);
        return;
    }

    inline uint32_t number_of_received_responses(vector<bool>& done){
        int ret = 0;
        for(auto it = done.begin(); it != done.end(); it++){
            if(*it){
                ret++;
            }
        }
        return ret;
    }

    /* This function will be used for all communication.
     * datacenters just have the information for servers
     */
    int failure_support_optimized(const string& operation, const string& key, const string& timestamp, const vector<string>& values, uint32_t RAs,
                                  vector<uint32_t> quorom, vector<uint32_t> servers, uint32_t total_num_servers, vector<DC*>& datacenters,
                                  const string current_class, const uint32_t conf_id, uint32_t timeout_per_request, vector<strVec> &ret){
        DPRINTF(DEBUG_CAS_Client, "started.\n");

        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with quorum size " + to_string(quorom.size()), DEBUG_CAS_Client);

        map<uint32_t, future<strVec> > responses; // server_id, future
        vector<bool> done(total_num_servers, false);
        ret.clear();

        int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
        RAs--;
        for(auto it = quorom.begin(); it != quorom.end(); it++){
            promise<strVec> prm;
            responses.emplace(*it, prm.get_future());
            thread(&_send_one_server, operation, move(prm), key, *(datacenters[*it]->servers[0]),
                        current_class, conf_id, values[*it], timestamp).detach();
        }

        EASY_LOG_M(string("requests were sent to Quorum with size of ") + to_string(quorom.size()));

        chrono::system_clock::time_point end = chrono::system_clock::now() +
                                                    chrono::milliseconds(timeout_per_request);
        auto it = responses.begin();
        while(true){
            if(done[it->first]){
                it++;
                if(it == responses.end())
                    it = responses.begin();
//                DPRINTF(DEBUG_CAS_Client, "one done skipped.\n");
                continue;
            }

//            DPRINTF(DEBUG_CAS_Client, "try one done.\n");
            if(it->second.valid() && it->second.wait_for(chrono::milliseconds(1)) == future_status::ready){
                strVec data = it->second.get();
                if(data.size() != 0){
                    ret.push_back(data);
                    done[it->first] = true;
                    DPRINTF(DEBUG_CAS_Client, "one done.\n");
                    if(number_of_received_responses(done) == quorom.size()){
                        EASY_LOG_M("Responses collected successfully");
                        op_status = 0;
                        break;
                    }
                }
                else{
                    // Access all the servers and wait for Q1.size() of them.
                    op_status = -1; // You should access all the server.
                    break;
                }
            }

            if(chrono::system_clock::now() > end){
                // Access all the servers and wait for Q1.size() of them.
                op_status = -1; // You should access all the server.
                break;
            }

            it++;
            if(it == responses.end())
                it = responses.begin();
            continue;
        }

        DPRINTF(DEBUG_CAS_Client, "op_status %d\n", op_status);

        while(op_status == -1 && RAs--) { // Todo: RAs cannot be more than 2 with this implementation
            EASY_LOG_M("at least one request failed. Try again...");
            op_status = 0;
            for (auto it = servers.begin(); it != servers.end(); it++) { // request to all servers
                if (responses.find(*it) != responses.end()) {
                    continue;
                }
                promise<strVec> prm;
                responses.emplace(*it, prm.get_future());
                thread(&_send_one_server, operation, move(prm), key, *(datacenters[*it]->servers[0]),
                            current_class, conf_id, values[*it], timestamp).detach();
            }
            EASY_LOG_M(string("requests were sent to all servers. Number of servers: ") + to_string(servers.size()));

            chrono::system_clock::time_point end = chrono::system_clock::now() +
                                                        chrono::milliseconds(timeout_per_request);
            auto it = responses.begin();
            while (true){
                if (done[it->first]){
                    it++;
                    if(it == responses.end())
                        it = responses.begin();
                    continue;
                }

                if(it->second.valid() &&
                   it->second.wait_for(chrono::milliseconds(1)) == future_status::ready){
                    strVec data = it->second.get();
                    if(data.size() != 0){
                        ret.push_back(data);
                        done[it->first] = true;
                        if(number_of_received_responses(done) == quorom.size()){
                            EASY_LOG_M("Responses collected successfully");
                            op_status = 0;
                            break;
                        }
                    }
                }

                if(chrono::system_clock::now() > end){
                    // Access all the servers and wait for Q1.size() of them.
                    EASY_LOG_M("Responses collected, FAILURE");
                    op_status = -1; // You should access all the server.
                    break;
                }

                it++;
                if(it == responses.end())
                    it = responses.begin();
                continue;
            }
        }

        return op_status;
    }
}

CAS_Client::CAS_Client(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts,
        uint32_t metadata_server_timeout, uint32_t timeout_per_request, vector<DC*>& datacenters,
        Client_Node* parent) : Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout,
        timeout_per_request, datacenters), parent(parent){
    assert(parent != nullptr);
    this->current_class = CAS_PROTOCOL_NAME;
}

CAS_Client::~CAS_Client(){
    DPRINTF(DEBUG_CAS_Client, "cliend with id \"%u\" has been destructed.\n", this->id);
}

int CAS_Client::get_timestamp(const string& key, unique_ptr<Timestamp>& timestamp_p, bool is_put){

    DPRINTF(DEBUG_CAS_Client, "started on key %s\n", key.c_str());

    vector<Timestamp> tss;
    timestamp_p.reset();

    const Placement& p = parent->get_placement(key);
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
    
    vector<strVec> ret;
    
    DPRINTF(DEBUG_CAS_Client, "calling failure_support_optimized.\n");
#ifndef No_GET_OPTIMIZED
    if(!is_put && p.quorums[this->local_datacenter_id].Q1.size() < p.quorums[this->local_datacenter_id].Q4.size()) {
        op_status = CAS_helper::failure_support_optimized("get_timestamp", key, "", vector<string>(p.m, ""), this->retry_attempts, p.quorums[this->local_datacenter_id].Q4,
                                                                 p.servers, p.m,
                                                                 this->datacenters, this->current_class,
                                                                 parent->get_conf_id(key),
                                                                 this->timeout_per_request, ret);
    }
    else{
        op_status = CAS_helper::failure_support_optimized("get_timestamp", key, "", vector<string>(p.m, ""), this->retry_attempts, p.quorums[this->local_datacenter_id].Q1,
                                                                 p.servers, p.m,
                                                                 this->datacenters, this->current_class,
                                                                 parent->get_conf_id(key),
                                                                 this->timeout_per_request, ret);
    }
#else
    op_status = CAS_helper::failure_support_optimized("get_timestamp", key, "", vector<string>(p.m, ""), this->retry_attempts,
                                                      p.quorums[this->local_datacenter_id].Q1, p.servers, p.m,
                                                      this->datacenters, this->current_class, parent->get_conf_id(key),
                                                      this->timeout_per_request, ret);
#endif

    DPRINTF(DEBUG_CAS_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            tss.emplace_back((*it)[1]);
            op_status = 0;   // For get_timestamp, even if one response Received operation is success
        }
        else if((*it)[0] == "operation_fail"){
            DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, (*it)[1]);
            op_status = -2; // reconfiguration happened on the key
            timestamp_p.reset();
            return S_RECFG;
        }
        else{
            assert(false);
        }
    }

    if(op_status == 0){
        timestamp_p.reset(new Timestamp(Timestamp::max_timestamp2(tss)));

        DPRINTF(DEBUG_CAS_Client, "finished successfully. Max timestamp received is %s\n",
                (timestamp_p)->get_string().c_str());

#ifndef No_GET_OPTIMIZED
        // Check if Q4 responses has the max timestamp
        uint32_t resp_counter = 0;
        for(uint32_t i = 0; i < tss.size(); i++) {
            if(tss[i] == *timestamp_p)
                resp_counter++;
        }

        if(resp_counter >= p.quorums[this->local_datacenter_id].Q4.size()){
            op_status = 10; // 10 means the get operation can be done in one phase
        }
#endif
    }
    else{
        DPRINTF(DEBUG_CAS_Client, "Operation Failed. op_status is %d\n", op_status);
        timestamp_p.reset();
        assert(false);
        return S_FAIL;
    }
    
    return op_status;
}

int CAS_Client::put(const string& key, const string& value){

    DPRINTF(DEBUG_CAS_Client, "started on key %s\n", key.c_str());
    EASY_LOG_INIT_M(string("on key ") + key);

//    Key_gaurd(this, key);
//    EASY_LOG_M("lock for the key granted");
    
    int le_counter = 0;
    uint64_t le_init = time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    const Placement& p = parent->get_placement(key);
    EASY_LOG_M("placement received. trying to get timestamp and encoding data simultaneously...");
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

    vector<string> chunks;
    future<int> encoder_ret = async(launch::async, &Liberasure::encode, &liberasure, ref(value), ref(chunks), p.m, p.k);
    
    DPRINTF(DEBUG_CAS_Client, "ts_latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

    unique_ptr<Timestamp> timestamp_p;
    unique_ptr<Timestamp> timestamp_tmp_p;
    int status = S_OK;
    status = this->get_timestamp(key, timestamp_tmp_p, true);
    if(status == S_RECFG){
        return parent->put(key, value);
    }
    if(timestamp_tmp_p){
        timestamp_p.reset(new Timestamp(timestamp_tmp_p->increase_timestamp(this->id)));
        timestamp_tmp_p.reset();
    }
    else{
        DPRINTF(DEBUG_CAS_Client, "get_timestamp operation failed key %s \n", key.c_str());
        assert(false);
    }

    EASY_LOG_M("timestamp received.");

    // Join the encoder thread
    if(encoder_ret.get() != 0){
        assert(false);
    }
    DPRINTF(DEBUG_CAS_Client, "ts_latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

    EASY_LOG_M("Data encoded. Trying to do phase 2...");


//    printf("sssssss: %lu\n", chunks[0]->size());
//    fflush(stdout);

    // prewrite
//    int i = 0;
//    char bbuf[1024*1024];
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
//    fflush(stdout);

    vector<strVec> ret;

    DPRINTF(DEBUG_CAS_Client, "calling failure_support_optimized.\n");
    op_status = CAS_helper::failure_support_optimized("put", key, timestamp_p->get_string(), chunks, this->retry_attempts, p.quorums[this->local_datacenter_id].Q2,
                                                      p.servers, p.m,
                                                     this->datacenters, this->current_class, parent->get_conf_id(key),
                                                     this->timeout_per_request, ret);

    DPRINTF(DEBUG_CAS_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            DPRINTF(DEBUG_CAS_Client, "OK received for key : %s\n", key.c_str());
        }
        else if((*it)[0] == "operation_fail"){
            DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, (*it)[1]);
//            op_status = -2; // reconfiguration happened on the key
//            return S_RECFG;
            return parent->put(key, value);
        }
        else{
            DPRINTF(DEBUG_CAS_Client, "Bad message received from server for key : %s\n", key.c_str());
            return -3; // Bad message received from server
        }
    }
    
    DPRINTF(DEBUG_CAS_Client, "latencies%d:pre %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

    if(op_status != 0){
        DPRINTF(DEBUG_CAS_Client, "pre_write could not succeed. op_status is %d\n", op_status);
        return -4; // pre_write could not succeed.
    }

    EASY_LOG_M("phase 2 done. Trying to do phase 3(FIN)...");

    // Fin

    DPRINTF(DEBUG_CAS_Client, "calling failure_support_optimized.\n");
    op_status = CAS_helper::failure_support_optimized("put_fin", key, timestamp_p->get_string(), vector<string>(p.m, ""),
                                                        this->retry_attempts, p.quorums[this->local_datacenter_id].Q3, p.servers, p.m,
                                                        this->datacenters, this->current_class, parent->get_conf_id(key),
                                                        this->timeout_per_request, ret);

    DPRINTF(DEBUG_CAS_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){

        }
        else if((*it)[0] == "operation_fail"){
            DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, (*it)[1]);
//            op_status = -2; // reconfiguration happened on the key
//            return S_RECFG;
            return parent->put(key, value);
        }
        else{
            op_status = -5; // Bad message received from server
            DPRINTF(DEBUG_CAS_Client, "Bad message received from server received for key : %s\n", key.c_str());
            return -5;
        }
    }

    if(op_status != 0){
        DPRINTF(DEBUG_CAS_Client, "Fin_write could not succeed. op_status is %d\n", op_status);
        return -6; // Fin_write could not succeed.
    }

    EASY_LOG_M("phase 3 done.");
    
    DPRINTF(DEBUG_CAS_Client, "end latencies%d:fin %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    return op_status;
}

int CAS_Client::get(const string& key, string& value){

    DPRINTF(DEBUG_CAS_Client, "started on key %s\n", key.c_str());

    EASY_LOG_INIT_M(string("on key ") + key);

//    Key_gaurd(this, key);
//    EASY_LOG_M("lock for the key granted");

    static map<string, string> cache_optimized_get; // key!timestamp -> value
    
    int le_counter = 0;
    uint64_t le_init = time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_CAS_Client, "ts_latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

    const Placement& p = parent->get_placement(key);
    EASY_LOG_M("placement received. trying to get timestamp...");
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

    bool uninitialized_key = false;
    value.clear();

    // Get the timestamp
    unique_ptr<Timestamp> timestamp_p;
    int status = S_OK;
    status = this->get_timestamp(key, timestamp_p);
    if(status == S_RECFG){
        EASY_LOG_M("key reconfigured. Trying again...");
        return parent->get(key, value);
    }
    if(!timestamp_p){
        DPRINTF(DEBUG_CAS_Client, "get_timestamp operation failed key %s \n", key.c_str());
        assert(false);
    }
    
    DPRINTF(DEBUG_CAS_Client, "ts_latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

    EASY_LOG_M("timestamp received. Trying to do phase 2...");

#ifndef No_GET_OPTIMIZED
    if(status == 10 && cache_optimized_get.find(key + "!" + timestamp_p->get_string()) != cache_optimized_get.end()){
        DPRINTF(DEBUG_CAS_Client, "get_optimized done \n");
        value = cache_optimized_get[key + "!" + timestamp_p->get_string()];
        DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);
        EASY_LOG_M("GET_OPTIMIZED: no need to do phase 2. Done.");
        return op_status;
    }
#endif
    
    // writeback
    vector<strVec> ret;
    vector<string> chunks;

    op_status = CAS_helper::failure_support_optimized("get", key, timestamp_p->get_string(), vector<string>(p.m, ""),
                                                             this->retry_attempts, p.quorums[this->local_datacenter_id].Q4,
                                                             p.servers, p.m, this->datacenters, this->current_class,
                                                             parent->get_conf_id(key),
                                                             this->timeout_per_request, ret);

    DPRINTF(DEBUG_CAS_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            if((*it)[1] == "Ack"){

            }
            else if((*it)[1] == "init"){
                uninitialized_key = true;
            }
            else{
                chunks.emplace_back((*it)[1]);
            }
        }
        else if((*it)[0] == "operation_fail"){
            DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, (*it)[1]);
//            op_status = -2; // reconfiguration happened on the key
//            return S_RECFG;
            return parent->get(key, value);
        }
        else{
            DPRINTF(DEBUG_CAS_Client, "wrong message received: %s : %s\n", (*it)[0].c_str(), (*it)[1].c_str());
            assert(false);
            op_status = -8; // Bad message received from server
        }
    }

    EASY_LOG_M(string("phase 2 done with status code: ") + to_string(op_status) + ". Trying to decode...");

    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

    if(op_status != 0){
        DPRINTF(DEBUG_CAS_Client, "get operation failed for key: %s op_status is %d\n", key.c_str(), op_status);
        return op_status;
    }

    if(!uninitialized_key){
        if(chunks.size() < p.k){
            op_status = -9;
            DPRINTF(DEBUG_CAS_Client, "chunks.size() < p.k key : %s\n", key.c_str());
            return op_status;
        }
        
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
        liberasure.decode(value, chunks, p.m, p.k);
    }
    else{
        value = "__Uninitiliazed";
    }

    EASY_LOG_M("Decode done.");
    DPRINTF(DEBUG_CAS_Client, "end latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);
    cache_optimized_get[key + "!" + timestamp_p->get_string()] = value;

    return op_status;
}
