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

namespace ABD_thread_helper{
    void _get_timestamp(std::promise <strVec>&& prm, std::string key, Server* server, std::string current_class,
            uint32_t conf_id){
        DPRINTF(DEBUG_ABD_Client, "started.\n");
        
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
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data = DataTransfer::deserialize(recvd);
        }
        else{
            data.clear();
        }

        prm.set_value(std::move(data));
        
        DPRINTF(DEBUG_ABD_Client, "finished.\n");
        return;
    }
    
    void _put(std::promise <strVec>&& prm, std::string key, std::string value, Timestamp timestamp, Server* server,
            std::string current_class, uint32_t conf_id){
        DPRINTF(DEBUG_ABD_Client, "started.\n");
        
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
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data = DataTransfer::deserialize(recvd);
        }
        else{
            data.clear();
        }

        prm.set_value(std::move(data));
        
        DPRINTF(DEBUG_ABD_Client, "finished with server port: %u\n", server->port);
        return;
    }
    
    void _get(std::promise <strVec>&& prm, std::string key, Server* server, std::string current_class,
            uint32_t conf_id){
        DPRINTF(DEBUG_ABD_Client, "started.\n");
        
        strVec data;
        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            prm.set_value(std::move(data));
            return;
        }
        
        data.push_back("get");
        data.push_back(key);
        data.push_back(current_class);
        data.push_back(std::to_string(conf_id));
        DataTransfer::sendMsg(*c, DataTransfer::serialize(data));
        
        data.clear();
        std::string recvd;
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data = DataTransfer::deserialize(recvd);
        }
        else{
            data.clear();
        }

        prm.set_value(std::move(data));
        
        DPRINTF(DEBUG_ABD_Client, "finished.\n");
        return;
    }
}

ABD_Client::ABD_Client(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts,
        uint32_t metadata_server_timeout, uint32_t timeout_per_request, std::vector<DC*>& datacenters,
        Client_Node* parent) : Client(id, local_datacenter_id, retry_attempts, metadata_server_timeout,
                timeout_per_request, datacenters){
    assert(parent != nullptr);
    this->parent = parent;
    this->current_class = ABD_PROTOCOL_NAME;

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

ABD_Client::~ABD_Client(){
#ifdef LOGGING_ON
    fclose(this->log_file);
#endif
    
    DPRINTF(DEBUG_ABD_Client, "cliend with id \"%u\" has been destructed.\n", this->id);
}

// get timestamp for write operation
int ABD_Client::get_timestamp(const std::string& key, Timestamp*& timestamp){
    DPRINTF(DEBUG_ABD_Client, "started.\n");

    int le_counter = 0;
    uint64_t le_init = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);


    const Placement& p = parent->get_placement(key);
    uint32_t RAs = this->retry_attempts;
    std::vector <Timestamp> tss;
    timestamp = nullptr;
    std::vector <std::future<strVec>> responses;
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
    
    responses.clear();
    tss.clear();
    RAs--;
    for(auto it = p.Q1.begin(); it != p.Q1.end(); it++){
        std::promise <strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(ABD_thread_helper::_get_timestamp, std::move(prm), key, datacenters[*it]->servers[0],
                this->current_class, parent->get_conf_id(key)).detach();
    }
    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    std::chrono::system_clock::time_point end =
            std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto& it:responses){
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
        }
        else if(data[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, stoul(data[1]));
//            assert(*p != nullptr);
            op_status = -2; // reconfiguration happened on the key
            timestamp = nullptr;
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

    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    uint32_t counter = ~((uint32_t)0);
    uint32_t num_fail_servers = 0;
    std::unordered_set <uint32_t> servers;
    
    set_intersection(p, servers);
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        tss.clear();
        counter = 0;
        num_fail_servers = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers
            
            std::promise <strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_get_timestamp, std::move(prm), key, datacenters[*it]->servers[0],
                    this->current_class, parent->get_conf_id(key)).detach();
        }
        
        std::chrono::system_clock::time_point end =
                std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
        
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
                if(num_fail_servers > p.f){
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
            }
            else if(data[0] == "operation_fail"){
                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
                parent->get_placement(key, true, stoul(data[1]));
//                assert(*p != nullptr);
                op_status = -2; // reconfiguration happened on the key
                timestamp = nullptr;
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
            
            if(counter >= p.Q1.size()){
                break;
            }
            
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
    
    
    ////////////////////



//    DPRINTF(DEBUG_ABD_Client, "started.\n");
//
//    std::vector<Timestamp> tss;
//    std::vector<std::string> vs;
//    *timestamp = nullptr;
//    std::vector<std::future<strVec> > responses;
//    int retries = this->prop->retry_attempts;
//    bool op_status = false;    //Success
//
//    while(!op_status && retries--){
//        for(auto it = p.Q1.begin();it != p.Q1.end(); it++){
//
//            std::promise<strVec> prm;
//            responses.emplace_back(prm.get_future());
//            std::thread(ABD_thread_helper::_get_timestamp, std::move(prm), *key,
//                            prop->datacenters[*it]->servers[0], this->current_class).detach();
//        }
//
//        for(auto &it:responses){
//            strVec data = it.get();
//
//            if(data[0] == "OK"){
//                printf("get timestamp for key :%s, ts: %s, value: %s\n", key->c_str(), data[2].c_str(), data[1].c_str());
//                
//                vs.emplace_back(data[1]);
//                tss.emplace_back(data[2]);
//                op_status = true;   // For get_timestamp, even if one response Received
//                                    // operation is success
//            }else if(data[0] == "operation_fail"){
//                update_placement(data[1]);
//                op_status = false;      // Retry the operation
//                printf("get_timestamp : operation failed received! for key : %s", key->c_str());
//                return S_RECFG;
//                //break;
//            }
//            //else : The server returned "Failed", that means the entry was not found
//            // We ignore the response in that case.
//            // The servers can return Failed for timestamp, which is acceptable
//        }
//
//        responses.clear();
//    }
//
//    if(op_status){
//        
//        uint32_t max_ts_i = Timestamp::max_timestamp3(tss);
//        *timestamp = new Timestamp(tss[max_ts_i]);
//        value = vs[max_ts_i];
//        
//        DPRINTF(DEBUG_ABD_Client, "finished successfully.\n");
//    }else{
//        DPRINTF(DEBUG_ABD_Client, "Operation Failed.\n");
//        return S_FAIL;
//    }
//
//    return S_OK;
}

int ABD_Client::put(const std::string& key, const std::string& value){
    
    DPRINTF(DEBUG_ABD_Client, "started.\n");

#ifdef LOGGING_ON
    gettimeofday (&log_tv, NULL);
    log_tm = localtime (&log_tv.tv_sec);
    strftime (log_fmt, sizeof (log_fmt), "%H:%M:%S:%%06u", log_tm);
    snprintf (log_buf, sizeof (log_buf), log_fmt, log_tv.tv_usec);
    fprintf(this->log_file, "%s write invoke %s\n", log_buf, value.c_str());
#endif

    int le_counter = 0;
    uint64_t le_init = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);


    const Placement& p = parent->get_placement(key);
    uint32_t RAs = this->retry_attempts;
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
    
//    DPRINTF(DEBUG_ABD_Client, "The value to be stored is %s \n", value.c_str());
    
    Timestamp* timestamp = nullptr;
    Timestamp* tmp = nullptr;
    int status = S_OK;
    std::vector <std::future<strVec>> responses;
    
    status = this->get_timestamp(key, tmp);
    
    if(tmp != nullptr){
        timestamp = new Timestamp(tmp->increase_timestamp(this->id));
        delete tmp;
        tmp = nullptr;
    }
    
    if(timestamp == nullptr){
//        free_chunks(chunks);
        
        if(status == S_RECFG){
            op_status = -2;
            return S_RECFG;
        }
        
        DPRINTF(DEBUG_ABD_Client, "get_timestamp operation failed key %s \n", key.c_str());
        
    }

    DPRINTF(DEBUG_CAS_Client, "put latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    // Put
    responses.clear();
    RAs--;
    for(auto it = p.Q2.begin(); it != p.Q2.end(); it++){
        std::promise <strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(ABD_thread_helper::_put, std::move(prm), key, value, *timestamp, datacenters[*it]->servers[0],
                this->current_class, parent->get_conf_id(key)).detach();
        
        DPRINTF(DEBUG_ABD_Client,
                "Issue _put request to key: %s and timestamp: %s and conf_id: %u and value_size :%lu \n", key.c_str(),
                timestamp->get_string().c_str(), parent->get_conf_id(key), value.size());
    }
    
    std::chrono::system_clock::time_point end =
            std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto& it:responses){
        
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
            DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
        }
        else if(data[0] == "operation_fail"){
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, stoul(data[1]));
//            assert(p != nullptr);
            op_status = -2; // reconfiguration happened on the key
            return S_RECFG;
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

    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    uint32_t counter = ~((uint32_t)0);
    uint32_t num_fail_servers;
    std::unordered_set <uint32_t> servers;
    
    set_intersection(p, servers);
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        counter = 0;
        num_fail_servers = 0;
        op_status = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers
            std::promise <strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_put, std::move(prm), key, value, *timestamp, datacenters[*it]->servers[0],
                    this->current_class, parent->get_conf_id(key)).detach();
            DPRINTF(DEBUG_ABD_Client,
                    "Issue _put request to key: %s and timestamp: %s and conf_id: %u and value_size :%lu \n",
                    key.c_str(), timestamp->get_string().c_str(), parent->get_conf_id(key), value.size());
        }
        
        std::chrono::system_clock::time_point end =
                std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
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
                if(num_fail_servers > p.f){
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
                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
                parent->get_placement(key, true, stoul(data[1]));
//                assert(p != nullptr);
                op_status = -2; // reconfiguration happened on the key
//                free_chunks(chunks);
                if(timestamp != nullptr){
                    delete timestamp;
                    timestamp = nullptr;
                }
                return S_RECFG;
            }
            else{
                DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
//                free_chunks(chunks);
                if(timestamp != nullptr){
                    delete timestamp;
                    timestamp = nullptr;
                }
                return -3; // Bad message received from server
            }
            
            if(counter >= p.Q2.size()){
                op_status = 0;
                break;
            }
        }
    }
    
    responses.clear();
    if(op_status != 0){
        DPRINTF(DEBUG_ABD_Client, "pre_write could not succeed\n");
        if(timestamp != nullptr){
            delete timestamp;
            timestamp = nullptr;
        }
        return -4; // pre_write could not succeed.
    }


    DPRINTF(DEBUG_CAS_Client, "fin latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    return op_status;
}

int ABD_Client::get(const std::string& key, std::string& value){

#ifdef LOGGING_ON
    gettimeofday (&log_tv, NULL);
    log_tm = localtime (&log_tv.tv_sec);
    strftime (log_fmt, sizeof (log_fmt), "%H:%M:%S:%%06u", log_tm);
    snprintf (log_buf, sizeof (log_buf), log_fmt, log_tv.tv_usec);
    fprintf(this->log_file, "%s read invoke nil\n", log_buf);
#endif

    bool can_be_optimized = false;

    int le_counter = 0;
    uint64_t le_init = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);


    DPRINTF(DEBUG_ABD_Client, "started.\n");
    
    value.clear();
    
    const Placement& p = parent->get_placement(key);
    
    uint32_t RAs = this->retry_attempts;
    
    std::vector <Timestamp> tss;
    std::vector <std::string> vs;
//    *timestamp = nullptr;
    uint32_t idx = -1;
    std::vector <std::future<strVec>> responses;
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
    
    
    responses.clear();
    tss.clear();
    vs.clear();
    RAs--;
    for(auto it = p.Q1.begin(); it != p.Q1.end(); it++){
        std::promise <strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(ABD_thread_helper::_get, std::move(prm), key, datacenters[*it]->servers[0], this->current_class,
                parent->get_conf_id(key)).detach();
    }

    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    std::chrono::system_clock::time_point end =
            std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto& it:responses){
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
            vs.emplace_back(data[2]);
            
            // Todo: make sure that the statement below is ture!
            op_status = 0;   // For get_timestamp, even if one response Received operation is success
        }
        else if(data[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, stoul(data[1]));
//            assert(p != nullptr);
            op_status = -2; // reconfiguration happened on the key
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

    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    uint32_t counter = ~((uint32_t)0);
    uint32_t num_fail_servers = 0;
    std::unordered_set <uint32_t> servers;
    
    set_intersection(p, servers);
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        tss.clear();
        vs.clear();
        counter = 0;
        num_fail_servers = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers
            std::promise <strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_get, std::move(prm), key, datacenters[*it]->servers[0], this->current_class,
                    parent->get_conf_id(key)).detach();
        }
        
        std::chrono::system_clock::time_point end =
                std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
        
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
                if(num_fail_servers > p.f){
                    op_status = -100; // You should access all the server.
                    break;
                }
                else{
                    continue;
                }
            }
            
            if(data[0] == "OK"){
                tss.emplace_back(data[1]);
                vs.emplace_back(data[2]);
                
                // Todo: make sure that the statement below is ture!
                op_status = 0;   // For get_timestamp, even if one response Received operation is success
                counter++;
            }
            else if(data[0] == "operation_fail"){
                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
                parent->get_placement(key, true, stoul(data[1]));
//                assert(p != nullptr);
                op_status = -2; // reconfiguration happened on the key
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
            
            if(counter >= p.Q1.size()){
                break;
            }
            
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

    DPRINTF(DEBUG_CAS_Client, "phase 1 fin, put latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

#ifndef No_GET_OPTIMIZED
    can_be_optimized = true;
    Timestamp temp = *(tss.begin());
    for(auto it = tss.begin() + 1; it != tss.end(); it++){
        if(!(temp == *it)){
            can_be_optimized = false;
            break;
        }
    }
    if(can_be_optimized){

        if(vs[idx] == "init"){
            value = "__Uninitiliazed";
        }
        else{
            value = vs[idx];
        }

        DPRINTF(DEBUG_CAS_Client, "get does not need to put. end latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

        return op_status;
    }
#endif

    // Put
    RAs = this->retry_attempts;
    responses.clear();
    RAs--;
    for(auto it = p.Q2.begin(); it != p.Q2.end(); it++){
        std::promise <strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(ABD_thread_helper::_put, std::move(prm), key, vs[idx], tss[idx], datacenters[*it]->servers[0],
                this->current_class, parent->get_conf_id(key)).detach();
        
        DPRINTF(DEBUG_ABD_Client,
                "Issue _put request to key: %s and timestamp: %s and conf_id: %u and chunk_size :%lu \n", key.c_str(),
                tss[idx].get_string().c_str(), parent->get_conf_id(key), vs[idx].size());
    }
    
    end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto& it:responses){
        
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
            DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
        }
        else if(data[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            parent->get_placement(key, true, stoul(data[1]));
//            assert(p != nullptr);
            op_status = -2; // reconfiguration happened on the key
            return S_RECFG;
        }
        else{
            DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
            return -3; // Bad message received from server
        }
    }

//    uint32_t counter;
//    std::unordered_set<uint32_t> servers;

//    set_intersection(p, servers);
    
    counter = ~((uint32_t)0);
//    uint32_t num_fail_servers;
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        counter = 0;
        num_fail_servers = 0;
        op_status = 0;
        
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers
            std::promise <strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_put, std::move(prm), key, vs[idx], tss[idx], datacenters[*it]->servers[0],
                    this->current_class, parent->get_conf_id(key)).detach();
            DPRINTF(DEBUG_ABD_Client,
                    "Issue _put request to key: %s and timestamp: %s and conf_id: %u and value_size :%lu \n",
                    key.c_str(), tss[idx].get_string().c_str(), parent->get_conf_id(key), vs[idx].size());
        }
        
        std::chrono::system_clock::time_point end =
                std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
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
                if(num_fail_servers > p.f){
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
                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
                parent->get_placement(key, true, stoul(data[1]));
//                assert(p != nullptr);
                op_status = -2; // reconfiguration happened on the key
//                free_chunks(chunks);
                return S_RECFG;
            }
            else{
                DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
//                free_chunks(chunks);
                return -3; // Bad message received from server
            }
            
            if(counter >= p.Q2.size()){
                op_status = 0;
                break;
            }
        }
    }

    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    responses.clear();
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

    DPRINTF(DEBUG_CAS_Client, "end latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);
    
    
    return op_status;
}

