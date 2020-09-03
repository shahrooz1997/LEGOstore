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
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <regex>
#include <future>
#include <sys/time.h>

template <typename T>
void set_intersection(Placement *p, std::unordered_set<T> &res){
    res.insert(p->Q1.begin(), p->Q1.end());
    res.insert(p->Q2.begin(), p->Q2.end());
}

namespace ABD_thread_helper{
    void _get_timestamp(std::promise<strVec> &&prm, std::string key, Server *server,
                        std::string current_class, uint32_t conf_id){

        DPRINTF(DEBUG_ABD_Client, "started.\n");

        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            return;
        }

        strVec data;
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
        
        DPRINTF(DEBUG_ABD_Client, "finished.\n");

        return;
    }
    
    void _put(std::promise<strVec> &&prm, std::string key, std::string value, Timestamp timestamp,
                        Server *server, std::string current_class, uint32_t conf_id){

        DPRINTF(DEBUG_ABD_Client, "started.\n");

        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            return;
        }

        strVec data;
        data.push_back("put");
        data.push_back(key);
        data.push_back(timestamp.get_string());
        data.push_back(value);
        data.push_back(current_class);
        data.push_back(std::to_string(conf_id));

        if((value).empty()){
                   printf("WARNING!!! SENDING EMPTY STRING TO SERVER\n");
        }
        DataTransfer::sendMsg(*c,DataTransfer::serialize(data));

        data.clear();
        std::string recvd;


        // If the socket recv itself fails, then 'promise' value will not be made available
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            data =  DataTransfer::deserialize(recvd);
            prm.set_value(std::move(data));
        }

        DPRINTF(DEBUG_ABD_Client, "finished with server port: %u\n", server->port);

        return;
    }
    
    void _get(std::promise<strVec> &&prm, std::string key, Server *server,
                        std::string current_class, uint32_t conf_id){

        DPRINTF(DEBUG_ABD_Client, "started.\n");

        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            return;
        }

        strVec data;
        data.push_back("get");
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
        
        DPRINTF(DEBUG_ABD_Client, "finished.\n");

        return;
    }
}

ABD_Client::ABD_Client(uint32_t client_id, uint32_t local_datacenter_id, std::vector <DC*> &datacenters, int *desc_l, std::map<std::string, std::pair<uint32_t, Placement> > *keys_info){
    assert(keys_info != nullptr);
    
    this->local_datacenter_id = local_datacenter_id;
    retry_attempts = DEFAULT_RET_ATTEMPTS;
    metadata_server_timeout = DEFAULT_METASER_TO;
    timeout_per_request = 10000;
    this->keys_info = keys_info;
    
    
    // Todo: what is this???
    start_time = 0;
    
    this->datacenters = datacenters;
    
    this->current_class = ABD_PROTOCOL_NAME;
    this->id = client_id;

    this->desc = desc_l;
    
//    this->operation_id = 0;
    
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

ABD_Client::~ABD_Client() {
#ifdef LOGGING_ON
    fclose(this->log_file);
#endif
    
    DPRINTF(DEBUG_ABD_Client, "cliend with id \"%u\" has been destructed.\n", this->id);
}

int ABD_Client::update_placement(std::string &key, uint32_t conf_id){
    int ret = 0;
    
    std::string status, msg;
    Placement* p;
    
//    DPRINTF(DEBUG_CAS_Client, "calling request_placement....\n");
    
    ret = request_placement(key, conf_id, status, msg, p, this->retry_attempts, this->metadata_server_timeout);

    assert(ret == 0);
    assert(status == "OK");
    
    auto it = keys_info->find(key);
    if(it == keys_info->end()){
        (*keys_info)[key] = std::pair<uint32_t, Placement>(stoul(msg), *p);
        if(p->protocol == CAS_PROTOCOL_NAME){
            if((*(this->desc)) != -1)
                destroy_liberasure_instance((*(this->desc)));
            (*(this->desc)) = create_liberasure_instance(p);
        }
        ret = 0;
    }
    else{
        uint32_t saved_conf_id = it->second.first;
        if(status == "OK" && std::to_string(saved_conf_id) != std::to_string(conf_id)){

            (*keys_info)[key] = std::pair<uint32_t, Placement>(stoul(msg), *p);
            ret =  0;
            if(p->protocol == CAS_PROTOCOL_NAME){
                if((*(this->desc)) != -1)
                    destroy_liberasure_instance((*(this->desc)));
                (*(this->desc)) = create_liberasure_instance(p);
            }
        }
        else{
            DPRINTF(DEBUG_CAS_Client, "msg is %s\n", msg.c_str());
            ret = -1;
        }

        if(ret == 0 && (*(this->desc)) == -1){
            (*(this->desc)) = create_liberasure_instance(p);
        }

        // Todo::: Maybe causes problem
        delete p;
        p = nullptr;

        return ret;
    }
    
    return ret;
}

Placement* ABD_Client::get_placement(std::string &key, bool force_update, uint32_t conf_id){
    
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

int ABD_Client::get_timestamp(std::string *key, Timestamp **timestamp, Placement **p){ // get timestamp for write operation

    DPRINTF(DEBUG_ABD_Client, "started.\n");

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
        std::thread(ABD_thread_helper::_get_timestamp, std::move(prm), *key,
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
        
        if(data[0] == "OK"){
            tss.emplace_back(data[1]);

            // Todo: make sure that the statement below is ture!
            op_status = 0;   // For get_timestamp, even if one response Received operation is success
        }else if(data[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key->c_str());
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
    
    uint32_t counter;
    std::unordered_set<uint32_t> servers;

    set_intersection(*p, servers);
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        tss.clear();
        counter = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_get_timestamp, std::move(prm), *key,
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

            if(data[0] == "OK"){
                tss.emplace_back(data[1]);

                // Todo: make sure that the statement below is ture!
                op_status = 0;   // For get_timestamp, even if one response Received operation is success
                counter++;
            }else if(data[0] == "operation_fail"){
                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key->c_str());
                *p = get_placement(*key, true, stoul(data[1]));
                assert(*p != nullptr);
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
            
            if(counter >= (*p)->Q1.size())
                break;
            
        }
    }

    if(op_status == 0){
        *timestamp = new Timestamp(Timestamp::max_timestamp2(tss));
        
        DPRINTF(DEBUG_ABD_Client, "finished successfully. Max timestamp received is %s\n", (*timestamp)->get_string().c_str());
    }else{
        DPRINTF(DEBUG_ABD_Client, "Operation Failed.\n");
        assert(false);
        return S_FAIL;
    }

    return S_OK;
    
    
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

    return S_OK;
}

int ABD_Client::put(std::string key, std::string value, bool insert){

    DPRINTF(DEBUG_ABD_Client, "started.\n");

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

    DPRINTF(DEBUG_ABD_Client, "The value to be stored is %s \n", value.c_str());

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

    if(timestamp == nullptr){
//        free_chunks(chunks);

        if(status == S_RECFG){
            op_status = -2;
            return S_RECFG;
        }

        DPRINTF(DEBUG_ABD_Client, "get_timestamp operation failed key %s \n", key.c_str());
        
    }
    
    // Put
    responses.clear();
    RAs--;
    for(auto it = p->Q2.begin(); it != p->Q2.end(); it++){
        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(ABD_thread_helper::_put, std::move(prm), key, value, *timestamp,
                                datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();

        DPRINTF(DEBUG_ABD_Client, "Issue _put request to key: %s and timestamp: %s and conf_id: %u and value_size :%lu \n", key.c_str(), timestamp->get_string().c_str(), (*(this->keys_info))[key].first, value.size());
    }
    
    std::chrono::system_clock::time_point end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto &it:responses){
        
        if(it.wait_until(end) == std::future_status::timeout){
            // Access all the servers and wait for Q1.size() of them.
            op_status = -1; // You should access all the server.
            break;
        }
        
        strVec data = it.get();
        

        if(data[0] == "OK"){
            DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
        }
        else if(data[0] == "operation_fail"){
            if(timestamp != nullptr){
                delete timestamp;
                timestamp = nullptr;
            }
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            p = get_placement(key, true, stoul(data[1]));
            assert(p != nullptr);
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
    
    uint32_t counter;
    std::unordered_set<uint32_t> servers;

    set_intersection(p, servers);
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        counter = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_put, std::move(prm), key, value, *timestamp,
                                datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();
            DPRINTF(DEBUG_ABD_Client, "Issue _put request to key: %s and timestamp: %s and conf_id: %u and value_size :%lu \n", key.c_str(), timestamp->get_string().c_str(), (*(this->keys_info))[key].first, value.size());
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

            if(data[0] == "OK"){
                counter++;
            }
            else if(data[0] == "operation_fail"){
                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
                p = get_placement(key, true, stoul(data[1]));
                assert(p != nullptr);
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
            
            if(counter >= p->Q2.size()){
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
    
    return op_status;
}

int ABD_Client::get(std::string key, std::string &value){

#ifdef LOGGING_ON
    gettimeofday (&log_tv, NULL);
    log_tm = localtime (&log_tv.tv_sec);
    strftime (log_fmt, sizeof (log_fmt), "%H:%M:%S:%%06u", log_tm);
    snprintf (log_buf, sizeof (log_buf), log_fmt, log_tv.tv_usec);
    fprintf(this->log_file, "%s read invoke nil\n", log_buf);
#endif
    
    DPRINTF(DEBUG_ABD_Client, "started.\n");
    
    value.clear();
    
    Placement *p = get_placement(key);

    uint32_t RAs = this->retry_attempts;
    
    std::vector<Timestamp> tss;
    std::vector<std::string> vs;
//    *timestamp = nullptr;
    uint32_t idx = -1;
    std::vector<std::future<strVec> > responses;
    int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
    
    
    responses.clear();
    tss.clear();
    RAs--;
    for(auto it = p->Q1.begin();it != p->Q1.end(); it++){

        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(ABD_thread_helper::_get, std::move(prm), key,
                            datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();
    }
    
    std::chrono::system_clock::time_point end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto &it:responses){
        if(it.wait_until(end) == std::future_status::timeout){
            // Access all the servers and wait for Q1.size() of them.
            op_status = -1; // You should access all the server.
            break;
        }
        
        strVec data = it.get();
        
        if(data[0] == "OK"){
            tss.emplace_back(data[1]);
            vs.emplace_back(data[2]);

            // Todo: make sure that the statement below is ture!
            op_status = 0;   // For get_timestamp, even if one response Received operation is success
        }else if(data[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            p = get_placement(key, true, stoul(data[1]));
            assert(p != nullptr);
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
    
    uint32_t counter;
    std::unordered_set<uint32_t> servers;

    set_intersection(p, servers);
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        tss.clear();
        vs.clear();
        counter = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_get, std::move(prm), key,
                            datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();
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

            if(data[0] == "OK"){
                tss.emplace_back(data[1]);
                vs.emplace_back(data[2]);

                // Todo: make sure that the statement below is ture!
                op_status = 0;   // For get_timestamp, even if one response Received operation is success
                counter++;
            }else if(data[0] == "operation_fail"){
                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
                p = get_placement(key, true, stoul(data[1]));
                assert(p != nullptr);
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
            
            if(counter >= (p)->Q1.size())
                break;
            
        }
    }

    if(op_status == 0){
        idx = Timestamp::max_timestamp3(tss);
    }else{
        DPRINTF(DEBUG_ABD_Client, "Operation Failed.\n");
        assert(false);
        return S_FAIL;
    }
    
    // Put
    RAs = this->retry_attempts;
    responses.clear();
    RAs--;
    for(auto it = p->Q2.begin(); it != p->Q2.end(); it++){
        std::promise<strVec> prm;
        responses.emplace_back(prm.get_future());
        std::thread(ABD_thread_helper::_put, std::move(prm), key, vs[idx], tss[idx],
                                datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();

        DPRINTF(DEBUG_ABD_Client, "Issue _put request to key: %s and timestamp: %s and conf_id: %u and chunk_size :%lu \n", key.c_str(), tss[idx].get_string().c_str(), (*(this->keys_info))[key].first, vs[idx].size());
    }
    
    end = std::chrono::system_clock::now() + std::chrono::milliseconds(this->timeout_per_request);
    for(auto &it:responses){
        
        if(it.wait_until(end) == std::future_status::timeout){
            // Access all the servers and wait for Q1.size() of them.
            op_status = -1; // You should access all the server.
            break;
        }
        
        strVec data = it.get();
        

        if(data[0] == "OK"){
            DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
        }
        else if(data[0] == "operation_fail"){
            DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
            p = get_placement(key, true, stoul(data[1]));
            assert(p != nullptr);
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
    
    while(op_status == -1 && RAs--){
        
        responses.clear(); // ignore responses to old requests
        counter = 0;
        for(auto it = servers.begin(); it != servers.end(); it++){ // request to all servers

            std::promise<strVec> prm;
            responses.emplace_back(prm.get_future());
            std::thread(ABD_thread_helper::_put, std::move(prm), key, vs[idx], tss[idx],
                                datacenters[*it]->servers[0], this->current_class, (*(this->keys_info))[key].first).detach();
            DPRINTF(DEBUG_ABD_Client, "Issue _put request to key: %s and timestamp: %s and conf_id: %u and value_size :%lu \n", key.c_str(), tss[idx].get_string().c_str(), (*(this->keys_info))[key].first, vs[idx].size());
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

            if(data[0] == "OK"){
                counter++;
            }
            else if(data[0] == "operation_fail"){
                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
                p = get_placement(key, true, stoul(data[1]));
                assert(p != nullptr);
                op_status = -2; // reconfiguration happened on the key
//                free_chunks(chunks);
                return S_RECFG;
            }
            else{
                DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
//                free_chunks(chunks);
                return -3; // Bad message received from server
            }
            
            if(counter >= p->Q2.size()){
                op_status = 0;
                break;
            }
        }
    }

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
    
    
    return op_status;
}

