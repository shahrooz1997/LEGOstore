/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   ABD_Client.cpp
 * Author: shahrooz
 * 
 * Created on August 14, 2020, 3:07 PM
 */

#include "ABD_Client.h"
#include "Client_Node.h"
#include "../inc/ABD_Client.h"

namespace ABD_helper{
    inline uint32_t number_of_received_responses(std::vector<bool>& done){
        int ret = 0;
        for(auto it = done.begin(); it != done.end(); it++){
            if(*it){
                ret++;
            }
        }
        return ret;
    }

    void _send_one_server(const std::string operation, std::promise <strVec>&& prm, const std::string key,
                          const Server* server, const std::string current_class, const uint32_t conf_id, const std::string value = "",
                          const std::string timestamp = ""){

        DPRINTF(DEBUG_ABD_Client, "started.\n");

        strVec data;
        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            prm.set_value(std::move(data));
            return;
        }

        data.push_back(operation); // get_timestamp, put, get
        data.push_back(key);
        if(operation == "put"){
            if(value.empty()){
                DPRINTF(DEBUG_CAS_Client, "WARNING!!! SENDING EMPTY STRING TO SERVER.\n");
            }
            data.push_back(timestamp);
            data.push_back(value);
        }
        else if(operation == "get"){

        }
        else if(operation == "get_timestamp"){

        }
        else{
            assert(false);
        }

        data.push_back(current_class);
        data.push_back(std::to_string(conf_id));

        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        data.clear();
        std::string recvd;
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data = DataTransfer::deserialize(recvd);
            prm.set_value(std::move(data));
        }
        else{
            data.clear();
            prm.set_value(std::move(data));
        }

        DPRINTF(DEBUG_CAS_Client, "finished successfully. with port: %u\n", server->port);
        return;
    }

    /* This function will be used for all communication.
     * datacenters just have the information for servers
     */
    int failure_support_optimized(const std::string& operation, const std::string& key, const std::string& timestamp, const std::string& value, uint32_t RAs,
                                  std::vector <uint32_t> quorom, std::unordered_set <uint32_t> servers, std::vector<DC*>& datacenters,
                                  const std::string current_class, const uint32_t conf_id, uint32_t timeout_per_request, std::vector<strVec> &ret){
        DPRINTF(DEBUG_CAS_Client, "started.\n");

        EASY_LOG_INIT_M(std::string("to do ") + operation + " on key " + key + " with quorum size " + std::to_string(quorom.size()), false);

        std::map <uint32_t, std::future<strVec> > responses; // server_id, future
        std::vector<bool> done(servers.size(), false);
        ret.clear();

        int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

        RAs--;
        for(auto it = quorom.begin(); it != quorom.end(); it++){
            std::promise <strVec> prm;
            responses.emplace(*it, prm.get_future());
            std::thread(&_send_one_server, operation, std::move(prm), key, datacenters[*it]->servers[0],
                        current_class, conf_id, value, timestamp).detach();
        }

        EASY_LOG_M("requests were sent to Quorum");

        std::chrono::system_clock::time_point end = std::chrono::system_clock::now() +
                std::chrono::milliseconds(timeout_per_request);
        auto it = responses.begin();
        while(true){
            if(done[it->first]){
                it++;
                if(it == responses.end())
                    it = responses.begin();
//                DPRINTF(DEBUG_CAS_Client, "one done skipped.\n");
                continue;
            }

            if(it->second.valid() && it->second.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready){
                strVec data = it->second.get();
                if(data.size() != 0){
                    ret.push_back(data);
                    done[it->first] = true;
                    DPRINTF(DEBUG_CAS_Client, "one done.\n");
                    if(number_of_received_responses(done) == quorom.size()){
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

            if(std::chrono::system_clock::now() > end){
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
                std::promise <strVec> prm;
                responses.emplace(*it, prm.get_future());
                std::thread(&_send_one_server, operation, std::move(prm), key, datacenters[*it]->servers[0],
                             current_class, conf_id, value, timestamp).detach();
            }
            EASY_LOG_M(std::string("requests were sent to all servers. Number of servers: ") + std::to_string(servers.size()));

            std::chrono::system_clock::time_point end = std::chrono::system_clock::now() +
                                                        std::chrono::milliseconds(timeout_per_request);
            auto it = responses.begin();
            while (true){
                if (done[it->first]){
                    it++;
                    if(it == responses.end())
                        it = responses.begin();
                    continue;
                }

                if(it->second.valid() &&
                    it->second.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready){
                    strVec data = it->second.get();
                    if(data.size() != 0){
                        ret.push_back(data);
                        done[it->first] = true;
                        if(number_of_received_responses(done) == quorom.size()){
                            op_status = 0;
                            break;
                        }
                    }
                }

                if(std::chrono::system_clock::now() > end){
                    // Access all the servers and wait for Q1.size() of them.
                    op_status = -1; // You should access all the server.
                    EASY_LOG_M("Responses collected, FAILURE");
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

ABD_Client::ABD_Client(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts,
        uint32_t metadata_server_timeout, uint32_t timeout_per_request, std::vector<DC*>& datacenters,
        Client_Node* parent) : Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout,
        timeout_per_request, datacenters){

    assert(parent != nullptr);
    this->parent = parent;
    this->current_class = ABD_PROTOCOL_NAME;
}

ABD_Client::~ABD_Client(){
    DPRINTF(DEBUG_ABD_Client, "cliend with id \"%u\" has been destructed.\n", this->id);
}

// get timestamp for write operation
int ABD_Client::get_timestamp(const std::string& key, Timestamp*& timestamp){

    DPRINTF(DEBUG_ABD_Client, "started on key %s\n", key.c_str());

    std::vector <Timestamp> tss;
    timestamp = nullptr;

    int le_counter = 0;
    uint64_t le_init = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

    const Placement& p = parent->get_placement(key);
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

    std::unordered_set <uint32_t> servers;
    set_intersection(p, servers);

    std::vector<strVec> ret;

    DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
    op_status = ABD_helper::failure_support_optimized("get_timestamp", key, "", "", this->retry_attempts, p.Q1, servers,
                                                 this->datacenters, this->current_class, parent->get_conf_id(key),
                                                 this->timeout_per_request, ret);

    DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            tss.emplace_back((*it)[1]);

            // Todo: make sure that the statement below is ture!
            op_status = 0;   // For get_timestamp, even if one response Received operation is success
        }
        else if((*it)[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, stoul((*it)[1]));
            op_status = -2; // reconfiguration happened on the key
            timestamp = nullptr;
            return S_RECFG;
        }
        else{
            assert(false);
        }
    }

    if(op_status == 0){
        timestamp = new Timestamp(Timestamp::max_timestamp2(tss));
        
        DPRINTF(DEBUG_ABD_Client, "finished successfully. Max timestamp received is %s\n",
                (timestamp)->get_string().c_str());
    }
    else{
        DPRINTF(DEBUG_CAS_Client, "Operation Failed. op_status is %d\n", op_status);
        assert(false);
        return S_FAIL;
    }

    DPRINTF(DEBUG_CAS_Client, "end latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    return op_status;
}

int ABD_Client::put(const std::string& key, const std::string& value){

    DPRINTF(DEBUG_ABD_Client, "started on key %s\n", key.c_str());

    EASY_LOG_INIT_M(std::string("on key ") + key);

    int le_counter = 0;
    uint64_t le_init = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

    const Placement& p = parent->get_placement(key);
    EASY_LOG_M("placement received. trying to get timestamp...");
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

    // Get the timestamp
    Timestamp* timestamp = nullptr;
    Timestamp* tmp = nullptr;
    int status = S_OK;
    status = this->get_timestamp(key, tmp);
    if(tmp != nullptr){
        timestamp = new Timestamp(tmp->increase_timestamp(this->id));
        delete tmp;
        tmp = nullptr;
    }
    if(timestamp == nullptr){
        if(status == S_RECFG){
            op_status = -2;
            return parent->put(key, value);
        }
        DPRINTF(DEBUG_ABD_Client, "get_timestamp operation failed key %s \n", key.c_str());
        assert(false);
    }

    EASY_LOG_M("timestamp received. Trying to do phase 2...");

    // put
    std::unordered_set <uint32_t> servers;
    set_intersection(p, servers);
    std::vector<strVec> ret;

    DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
    op_status = ABD_helper::failure_support_optimized("put", key, timestamp->get_string(), value, this->retry_attempts, p.Q2, servers,
                                                             this->datacenters, this->current_class, parent->get_conf_id(key),
                                                             this->timeout_per_request, ret);

    DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {

        if((*it)[0] == "OK"){
            DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
        }
        else if((*it)[0] == "operation_fail"){
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, stoul((*it)[1]));
//            assert(p != nullptr);
            op_status = -2; // reconfiguration happened on the key
//            return S_RECFG;
            return parent->put(key, value);
        }
        else{
            DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            return -3; // Bad message received from server
        }
    }

    DPRINTF(DEBUG_CAS_Client, "put latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

    if(op_status != 0){
        DPRINTF(DEBUG_ABD_Client, "pre_write could not succeed\n");
        if(timestamp != nullptr){
            delete timestamp;
            timestamp = nullptr;
        }
        return -4; // pre_write could not succeed.
    }

    EASY_LOG_M("phase 2 done.");

    DPRINTF(DEBUG_CAS_Client, "fin latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    return op_status;
}

int ABD_Client::get(const std::string& key, std::string& value){

    DPRINTF(DEBUG_ABD_Client, "started on key %s\n", key.c_str());

    EASY_LOG_INIT_M(std::string("on key ") + key);

    int le_counter = 0;
    uint64_t le_init = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    value.clear();
    
    const Placement& p = parent->get_placement(key);
    EASY_LOG_M("placement received. trying to do phase 1...");
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

    std::vector <Timestamp> tss;
    std::vector <std::string> vs;
    uint32_t idx = -1;
    std::unordered_set <uint32_t> servers;
    set_intersection(p, servers);
    std::vector<strVec> ret;

    DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
#ifndef No_GET_OPTIMIZED
    if(p.Q1.size() < p.Q2.size()) {
        op_status = ABD_helper::failure_support_optimized("get", key, "", "", this->retry_attempts, p.Q2,
                                                                 servers,
                                                                 this->datacenters, this->current_class,
                                                                 parent->get_conf_id(key),
                                                                 this->timeout_per_request, ret);
    }
    else{
        op_status = ABD_helper::failure_support_optimized("get", key, "", "", this->retry_attempts, p.Q1,
                                                                 servers,
                                                                 this->datacenters, this->current_class,
                                                                 parent->get_conf_id(key),
                                                                 this->timeout_per_request, ret);
    }
#else
    op_status = ABD_helper::failure_support_optimized("get", key, "", "", this->retry_attempts, p.Q1, servers,
                                                             this->datacenters, this->current_class, parent->get_conf_id(key),
                                                             this->timeout_per_request, ret);
#endif

    DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            tss.emplace_back((*it)[1]);
            vs.emplace_back((*it)[2]);

            // Todo: make sure that the statement below is ture!
            op_status = 0;   // For get_timestamp, even if one response Received operation is success
        }
        else if((*it)[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, stoul((*it)[1]));
            op_status = -2; // reconfiguration happened on the key
//            return S_RECFG;
            return parent->get(key, value);
            //break;
        }
        else{
            assert(false);
        }
        //else : The server returned "Failed", that means the entry was not found
        // We ignore the response in that case.
        // The servers can return Failed for timestamp, which is acceptable
    }
    
    if(op_status == 0){
        idx = Timestamp::max_timestamp3(tss);
    }
    else{
        DPRINTF(DEBUG_ABD_Client, "Operation Failed.\n");
        assert(false);
        return S_FAIL;
    }

    EASY_LOG_M("phase 1 done. Trying to do phase 2...");

    DPRINTF(DEBUG_CAS_Client, "phase 1 fin, put latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

#ifndef No_GET_OPTIMIZED
    // Check if Q2 responses has the max timestamp
    uint32_t resp_counter = 0;
    for(uint32_t i = 0; i < tss.size(); i++) {
        if(tss[i] == tss[idx])
            resp_counter++;
    }

    if(resp_counter >= p.Q2.size()){

        if(vs[idx] == "init"){
            value = "__Uninitiliazed";
        }
        else{
            value = vs[idx];
        }

        EASY_LOG_M("GET_OPTIMIZED: no need to do phase 2. Done.");

        DPRINTF(DEBUG_CAS_Client, "get does not need to put. end latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

        return op_status;
    }
#endif

    // Put
    op_status = ABD_helper::failure_support_optimized("put", key, tss[idx].get_string(), vs[idx], this->retry_attempts, p.Q2,
                                                             servers, this->datacenters, this->current_class,
                                                             parent->get_conf_id(key),
                                                             this->timeout_per_request, ret);

    DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
        }
        else if((*it)[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, stoul((*it)[1]));
//            assert(p != nullptr);
            op_status = -2; // reconfiguration happened on the key
//            return S_RECFG;
            return parent->get(key, value);
        }
        else{
            DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
            return -3; // Bad message received from server
        }
    }

    if(op_status != 0){
        DPRINTF(DEBUG_ABD_Client, "pre_write could not succeed\n");
        return -4; // pre_write could not succeed.
    }
    
    if(vs[idx] == "init"){
        value = "__Uninitiliazed";
    }
    else{
        value = vs[idx];
    }

    EASY_LOG_M("phase 2 done.");

    DPRINTF(DEBUG_CAS_Client, "end latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    return op_status;
}

