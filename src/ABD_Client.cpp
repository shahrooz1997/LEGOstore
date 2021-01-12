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

std::map <std::string, std::vector<uint32_t>>  additional_configs;
std::mutex config_list_lock;

namespace ABD_helper{
    inline uint32_t number_of_received_responses(vector<bool>& done){
        int ret = 0;
        for(auto it = done.begin(); it != done.end(); it++){
            if(*it){
                ret++;
            }
        }
        return ret;
    }

    void _send_one_server(const string operation, promise <strVec>&& prm, const string key,
                          const Server server, const string current_class, const uint32_t conf_id, const string value = "",
                          const string timestamp = ""){

        DPRINTF(DEBUG_ABD_Client, "started..\n", 0);
        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with conf_id " + to_string(conf_id), DEBUG_ABD_Client);

        strVec data;
        Connect c(server.ip, server.port);
        if(!c.is_connected()){
            prm.set_value(move(data));
            return;
        }

        EASY_LOG_M(string("Connected to ") + server.ip + ":" + to_string(server.port));

        data.push_back(operation); // get_timestamp, put, get
        data.push_back(key);
        if(operation == "put"){
            if(value.empty()){
                DPRINTF(DEBUG_ABD_Client, "WARNING!!! SENDING EMPTY STRING TO SERVER.\n");
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
        data.push_back(to_string(conf_id));

        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));

        EASY_LOG_M("request sent");

        data.clear();
        string recvd;
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data = DataTransfer::deserialize(recvd);
            if(data[0] == "OK") {
                std::string cfs;
                if(operation == "put"){
                    cfs = data[1];
                } else if(operation == "get"){
                    cfs = data[3];
                } else if(operation == "get_timestamp"){
                    cfs = data[2];
                }
                size_t pos = 0;
                std::string token;
                std::cout << "gkc5188: " << cfs << endl;
                if (cfs.find('!') != std::string::npos) {
                    while ((pos = cfs.find("!")) != std::string::npos) {
                        token = cfs.substr(0, pos);
                        config_list_lock.lock();
                        if(find(additional_configs[key].begin(), additional_configs[key].end(), (stoul(token))) !=
                                additional_configs[key].end()) {
                            additional_configs[key].push_back(stoul(token));
                        }
                        config_list_lock.unlock();
                    }
                }
            } 
            prm.set_value(std::move(data));
        }
        else{
            data.clear();
            EASY_LOG_M("response failed");
            prm.set_value(move(data));
        }

        DPRINTF(DEBUG_ABD_Client, "finished successfully. with port: %u\n", server.port);
        return;
    }
    
    
    /* This function will be used for all communication.
     * datacenters just have the information for servers
     */
    void failure_support_optimized(const std::string& operation, const std::string& key,
                                    const std::string& timestamp, const std::string& value, uint32_t RAs,
                                    std::vector<uint32_t> quorom, vector<uint32_t> servers, uint32_t total_num_servers,
                                    std::vector<DC*>& datacenters, const std::string current_class, 
                                    const uint32_t conf_id, uint32_t timeout_per_request, 
                                    std::vector<strVec> &ret, std::promise <std::pair<int, 
                                    std::vector<strVec>>> && parent_prm){
        DPRINTF(DEBUG_ABD_Client, "started.\n");

        EASY_LOG_INIT_M(string("to do ") + operation + " on key " + key + " with quorum size " + to_string(quorom.size()), DEBUG_ABD_Client);

        map <uint32_t, future<strVec> > responses; // server_id, future
        vector<bool> done(total_num_servers, false);
        ret.clear();

        int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
        RAs--;
        for(auto it = quorom.begin(); it != quorom.end(); it++){
            promise <strVec> prm;
            responses.emplace(*it, prm.get_future());
            thread(&_send_one_server, operation, move(prm), key, *(datacenters[*it]->servers[0]),
                        current_class, conf_id, value, timestamp).detach();
        }

        EASY_LOG_M("requests were sent to Quorum");

        chrono::system_clock::time_point end = chrono::system_clock::now() +
                chrono::milliseconds(timeout_per_request);
        auto it = responses.begin();
        while(true){
            if(done[it->first]){
                it++;
                if(it == responses.end())
                    it = responses.begin();
//                DPRINTF(DEBUG_ABD_Client, "one done skipped.\n");
                continue;
            }

            if(it->second.valid() && it->second.wait_for(chrono::milliseconds(1)) == future_status::ready){
                strVec data = it->second.get();
                if(data.size() != 0){
                    ret.push_back(data);
                    done[it->first] = true;
//                    DPRINTF(DEBUG_ABD_Client, "one done.\n");
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

        DPRINTF(DEBUG_ABD_Client, "op_status %d\n", op_status);

        while(op_status == -1 && RAs--) { // Todo: RAs cannot be more than 2 with this implementation
            EASY_LOG_M("at least one request failed. Try again...");
            op_status = 0;
            for (auto it = servers.begin(); it != servers.end(); it++) { // request to all servers
                if (responses.find(*it) != responses.end()) {
                    continue;
                }
                promise <strVec> prm;
                responses.emplace(*it, prm.get_future());
                thread(&_send_one_server, operation, move(prm), key, *(datacenters[*it]->servers[0]),
                             current_class, conf_id, value, timestamp).detach();
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
        std::pair<int, std::vector<strVec>> ret_obj;
        ret_obj.first = op_status;
        ret_obj.second = ret;
        parent_prm.set_value(std::move(ret_obj));
        return;
    }
    
    int do_operation(const std::string& operation, const std::string& key, const std::string& timestamp,
                        const std::string& value, uint32_t RAs, std::vector <uint32_t> quorom,
                        std::vector <uint32_t> servers, uint32_t total_num_servers, uint32_t local_datacenter_id,
                        std::vector<DC*>& datacenters,
                        const std::string current_class, const uint32_t conf_id, uint32_t timeout_per_request, 
                        std::vector<strVec>& ret, Client_Node* parent){
        DPRINTF(DEBUG_ABD_Client, "Daemon started.\n");
        std::promise <std::pair<int, std::vector<strVec>>> prm;
        std::future<std::pair<int, std::vector<strVec>>> fut = prm.get_future();
        std::map<uint32_t, bool> secondary_configs_map;
        std::map<uint32_t, std::future<std::pair<int, std::vector<strVec>>>> future_map;
        std::map<uint32_t, std::vector<strVec>> response_map;
        std::vector<bool> check_status; 

        std::thread(&failure_support_optimized, operation, key, timestamp, value, RAs, quorom, servers, total_num_servers,
                                                 std::ref(datacenters), current_class, conf_id,
                                                 timeout_per_request, std::ref(ret), std::move(prm)).detach();
//        bool config_found = false;
        std::vector<std::vector<strVec> > newrets;
        while ((additional_configs.find(key) != additional_configs.end()) && 
                (check_status.size() == additional_configs[key].size()) && 
                    !(fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready)) {
            for(auto it = additional_configs[key].begin(); it != additional_configs[key].end(); it++){
                if(!secondary_configs_map[*it]) {
                    // get placements
                    const Placement& p = parent->get_placement(key, false, *it);
        
//                    std::vector<strVec> newret;
                    newrets.emplace_back();
                    std::promise <std::pair<int, std::vector<strVec>>> prm_child;
                    future_map.emplace(*it, prm_child.get_future());
                    std::thread(&failure_support_optimized, "put", key, "", value, RAs, p.quorums[local_datacenter_id].Q2, 
                                p.servers, p.m, std::ref(datacenters), current_class, *it,
                                timeout_per_request, std::ref(newrets.back()), std::move(prm_child)).detach();
                    secondary_configs_map[*it] = true;             
                } else if(future_map[*it].valid() && future_map[*it].wait_for(std::chrono::milliseconds(1)) == std::future_status::ready){
                    std::pair<int, std::vector<strVec>> ret_obj = future_map[*it].get();

                    for(auto it2 = ret_obj.second.begin(); it2 != ret_obj.second.end(); it2++) {
                        if(ret_obj.first == -1) {
                            return ret_obj.first;
                        }
                        if((*it2)[0] == "OK"){
                            DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
                            
                        } else if((*it2)[0] == "operation_fail"){
                            config_list_lock.lock();
                            additional_configs[key].clear();
                            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
                            ret = ret_obj.second;
                            config_list_lock.unlock();
                            return -2; // reconfiguration happened on the key
                        }else{
                            DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
                            ret = ret_obj.second;
                            return -3; // Bad message received from server
                        }
                    }
                }
            }
        }

        std::pair<int, std::vector<strVec>> parent_op_status = fut.get();
        return parent_op_status.first; 
    }
}

ABD_Client::ABD_Client(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
        uint32_t timeout_per_request, vector<DC*>& datacenters, Client_Node* parent) :
        Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout,
        timeout_per_request, datacenters), parent(parent){
    assert(parent != nullptr);
    this->current_class = ABD_PROTOCOL_NAME;
    DPRINTF(DEBUG_ABD_Client, "client with id \"%u\" has been started.\n", this->id);
}

ABD_Client::~ABD_Client(){
    DPRINTF(DEBUG_ABD_Client, "cliend with id \"%u\" has been destructed.\n", this->id);
}

// get timestamp for write operation
int ABD_Client::get_timestamp(const string& key, unique_ptr<Timestamp>& timestamp_p){

    DPRINTF(DEBUG_ABD_Client, "started on key %s\n", key.c_str());

    vector<Timestamp> tss;
    timestamp_p.reset();

    int le_counter = 0;
    uint64_t le_init = time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_ABD_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

    const Placement& p = parent->get_placement(key);
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

    vector<strVec> ret;
    DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
    std::promise <std::pair<int, std::vector<strVec>>> prm;
    std::future<std::pair<int, std::vector<strVec>>> fut = prm.get_future();
    std::thread(&ABD_helper::failure_support_optimized, "get_timestamp", key, "", "", this->retry_attempts, p.quorums[this->local_datacenter_id].Q1, p.servers, p.m,
                                                 std::ref(this->datacenters), this->current_class, parent->get_conf_id(key),
                                                 this->timeout_per_request, std::ref(ret), std::move(prm)).detach();

    std::pair<int, std::vector<strVec>> ret_obj = fut.get();
    op_status = ret_obj.first;
    DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            tss.emplace_back((*it)[1]);
            op_status = 0;   // For get_timestamp, even one OK response suffices.
        }
        else if((*it)[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, (*it)[1]);
            config_list_lock.lock();
            additional_configs = parent->secondary_configs;
            op_status = -2; // reconfiguration happened on the key
            timestamp_p.reset();
            config_list_lock.unlock();
            return S_RECFG;
        }
        else{
            assert(false);
        }
    }

    if(op_status == 0){
        timestamp_p.reset(new Timestamp(Timestamp::max_timestamp2(tss)));
        
        DPRINTF(DEBUG_ABD_Client, "finished successfully. Max timestamp received is %s\n",
                timestamp_p->get_string().c_str());
    }
    else{
        DPRINTF(DEBUG_ABD_Client, "Operation Failed. op_status is %d\n", op_status);
        timestamp_p.reset();
        assert(false);
        return S_FAIL;
    }

    DPRINTF(DEBUG_ABD_Client, "end latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    return op_status;
}

int ABD_Client::put(const string& key, const string& value){

    DPRINTF(DEBUG_ABD_Client, "started on key %s\n", key.c_str());

    EASY_LOG_INIT_M(string("on key ") + key);

    Key_gaurd(this, key);
    EASY_LOG_M("lock for the key granted");

    int le_counter = 0;
    uint64_t le_init = time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_ABD_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

    const Placement& p = parent->get_placement(key);
    EASY_LOG_M("placement received. trying to get timestamp...");
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

    // Get the timestamp
    unique_ptr<Timestamp> timestamp_p;
    unique_ptr<Timestamp> timestamp_tmp_p;
    int status = S_OK;
    status = this->get_timestamp(key, timestamp_tmp_p);
    if(status == S_RECFG){
        return parent->put(key, value);
    }
    if(timestamp_tmp_p){
        timestamp_p.reset(new Timestamp(timestamp_tmp_p->increase_timestamp(this->id)));
        timestamp_tmp_p.reset();
    }
    else{
        DPRINTF(DEBUG_ABD_Client, "get_timestamp operation failed key %s \n", key.c_str());
        assert(false);
    }

    EASY_LOG_M("timestamp received. Trying to do phase 2...");

    // put
    vector<strVec> ret;

    DPRINTF(DEBUG_ABD_Client, "calling do_operation.\n");
    op_status = ABD_helper::do_operation("put", key, timestamp_p->get_string(), value, this->retry_attempts,
                                        p.quorums[this->local_datacenter_id].Q2, p.servers, p.m, this->local_datacenter_id,
                                        this->datacenters, this->current_class, parent->get_conf_id(key),
                                        this->timeout_per_request, ret, parent);

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
            parent->get_placement(key, true, (*it)[1]);
            config_list_lock.lock();
            additional_configs = parent->secondary_configs;
            config_list_lock.unlock();
//            assert(p != nullptr);
//            op_status = -2; // reconfiguration happened on the key
//            return S_RECFG;
            return parent->put(key, value);
        }
        else{
            DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
            return -3; // Bad message received from server
        }
    }

    DPRINTF(DEBUG_ABD_Client, "put latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

    if(op_status != 0){
        DPRINTF(DEBUG_ABD_Client, "pre_write could not succeed\n");
        return -4; // pre_write could not succeed.
    }

    EASY_LOG_M("phase 2 done.");

    DPRINTF(DEBUG_ABD_Client, "fin latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);
    return op_status;
}

int ABD_Client::get(const string& key, string& value){

    DPRINTF(DEBUG_ABD_Client, "started on key %s\n", key.c_str());

    EASY_LOG_INIT_M(string("on key ") + key);

    Key_gaurd(this, key);
    EASY_LOG_M("lock for the key granted");

    int le_counter = 0;
    uint64_t le_init = time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_ABD_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    value.clear();
    
    const Placement& p = parent->get_placement(key);
    EASY_LOG_M("placement received. trying to do phase 1...");
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)

    vector<Timestamp> tss;
    vector<string> vs;
    uint32_t idx = -1;
    vector<strVec> ret;

    DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
    std::promise <std::pair<int, std::vector<strVec>>> prm;
    std::future<std::pair<int, std::vector<strVec>>> fut = prm.get_future();
#ifndef No_GET_OPTIMIZED
    if(p.quorums[this->local_datacenter_id].Q1.size() < p.quorums[this->local_datacenter_id].Q2.size()) {
        std::thread(&ABD_helper::failure_support_optimized, "get", key, "", "", this->retry_attempts, p.quorums[this->local_datacenter_id].Q2,
                                                                 p.servers, p.m,
                                                                 std::ref(this->datacenters), this->current_class,
                                                                 parent->get_conf_id(key),
                                                                 this->timeout_per_request, std::ref(ret), std::move(prm)).detach();
    }   
    else{
        std::thread(&ABD_helper::failure_support_optimized, "get", key, "", "", this->retry_attempts, p.quorums[this->local_datacenter_id].Q1,
                                                                 p.servers, p.m,
                                                                 std::ref(this->datacenters), this->current_class,
                                                                 parent->get_conf_id(key),
                                                                 this->timeout_per_request, std::ref(ret), std::move(prm)).detach();
    }
#else
    std::thread(&ABD_helper::failure_support_optimized, "get", key, "", "", this->retry_attempts, p.quorums[this->local_datacenter_id].Q1,
                                                                 p.servers, p.m,
                                                             std::ref(this->datacenters), this->current_class, parent->get_conf_id(key),
                                                             this->timeout_per_request, std::ref(ret), std::move(prm)).detach();
#endif

    std::pair<int, std::vector<strVec>> ret_obj = fut.get();
    op_status = ret_obj.first;
    
    DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            tss.emplace_back((*it)[1]);
            vs.emplace_back((*it)[2]);
            op_status = 0;   // For get_timestamp, even if one response Received operation is success
        }
        else if((*it)[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, (*it)[1]);
            config_list_lock.lock();
            additional_configs = parent->secondary_configs;
            op_status = -2; // reconfiguration happened on the key
            config_list_lock.unlock();
//            return S_RECFG;
            return parent->get(key, value);
        }
        else{
            assert(false);
        }
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

    DPRINTF(DEBUG_ABD_Client, "phase 1 fin, put latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);

#ifndef No_GET_OPTIMIZED
    // Check if Q2 responses has the max timestamp
    uint32_t resp_counter = 0;
    for(uint32_t i = 0; i < tss.size(); i++) {
        if(tss[i] == tss[idx])
            resp_counter++;
    }
    if(resp_counter >= p.quorums[this->local_datacenter_id].Q2.size()){
        if(vs[idx] == "init"){
            value = "__Uninitiliazed";
        }
        else{
            value = vs[idx];
        }
        EASY_LOG_M("GET_OPTIMIZED: no need to do phase 2. Done.");
        DPRINTF(DEBUG_ABD_Client, "get does not need to put. end latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);
        return op_status;
    }
#endif

    // Put
    DPRINTF(DEBUG_ABD_Client, "calling do_operation in get.\n");
    op_status = ABD_helper::do_operation("put", key, tss[idx].get_string(), vs[idx], this->retry_attempts,
                                        p.quorums[this->local_datacenter_id].Q2, p.servers, p.m, this->local_datacenter_id,
                                        this->datacenters, this->current_class, parent->get_conf_id(key),
                                        this->timeout_per_request, ret, parent);
    
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
            parent->get_placement(key, true, (*it)[1]);
            config_list_lock.lock();
            additional_configs = parent->secondary_configs;
//            assert(p != nullptr);
            op_status = -2; // reconfiguration happened on the key
            config_list_lock.unlock();
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
    DPRINTF(DEBUG_ABD_Client, "end latencies%d: %lu\n", le_counter++, time_point_cast<chrono::milliseconds>(chrono::system_clock::now()).time_since_epoch().count() - le_init);
    return op_status;
}

