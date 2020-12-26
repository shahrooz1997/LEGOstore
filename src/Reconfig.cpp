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

#include "../inc/Reconfig.h"
#include <algorithm>
#include <future>
#include <unordered_set>

using std::max;

namespace liberasure_recon{
    void encode(const std::string* const data, std::vector<std::string*>* chunks, struct ec_args* const args, int desc){

        int rc = 0;
        //int desc = -1;
        char* orig_data = NULL;
        char** encoded_data = NULL, ** encoded_parity = NULL;
        uint64_t encoded_fragment_len = 0;

        DPRINTF(DEBUG_CAS_Client, "222222desc is %d\n", desc);

        //desc = liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, args);

        if(-EBACKENDNOTAVAIL == desc){
            fprintf(stderr, "Backend library not available!\n");
            return;
        }
        else if((args->k + args->m) > EC_MAX_FRAGMENTS){
            assert(-EINVALIDPARAMS == desc);
            return;
        }
        else{
            assert(desc > 0);
        }

        orig_data = (char*)data->c_str();
        assert(orig_data != NULL);
        rc = liberasurecode_encode(desc, orig_data, data->size(), &encoded_data, &encoded_parity,
                                   &encoded_fragment_len);
        DPRINTF(DEBUG_CAS_Client, "rc is %d\n", rc);
        fflush(stdout);
        assert(0 == rc); // ToDo: Add a lot of crash handler...

        for(int i = 0; i < args->k + args->m; i++){
            //int cmp_size = -1;
            //char *data_ptr = NULL;
            char* frag = NULL;

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

    int create_frags_array(char*** array, char** data, char** parity, struct ec_args* args, int* skips){
//         N.B. this function sets pointer reference to the ***array
//         from **data and **parity so DO NOT free each value of
//         the array independently because the data and parity will
//         be expected to be cleanup via liberasurecode_encode_cleanup
        int num_frags = 0;
        int i = 0;
        char** ptr = NULL;
        *array = (char**)malloc((args->k + args->m) * sizeof(char*));
        if(array == NULL){
            num_frags = -1;
            goto out;
        }
        //add data frags
        ptr = *array;
        for(i = 0; i < args->k; i++){
            if(data[i] == NULL || skips[i] == 1){
                //printf("%d skipped1\n", i);
                continue;
            }
            *ptr++ = data[i];
            num_frags++;
//            DPRINTF(DEBUG_CAS_Client, "num_frags is %d.\n", num_frags);
        }
        //add parity frags
        for(i = 0; i < args->m; i++){
            if(parity[i] == NULL || skips[i + args->k] == 1){
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

    void decode(std::string* data, std::vector<std::string*>* chunks, struct ec_args* const args, int desc){
        int i = 0;
        int rc = 0;
        //int desc = -1;
        char* orig_data = NULL;
        char** encoded_data = new char* [args->k], ** encoded_parity = new char* [args->m];
        uint64_t decoded_data_len = 0;
        char* decoded_data = NULL;
        char** avail_frags = NULL;
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

        if(-EBACKENDNOTAVAIL == desc){
            fprintf(stderr, "Backend library not available!\n");
            return;
        }
        else if((args->k + args->m) > EC_MAX_FRAGMENTS){
            assert(-EINVALIDPARAMS == desc);
            return;
        }
        else{
            assert(desc > 0);
        }

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

        int* skip = new int[args->k + args->m];
        for(int i = 0; i < args->k; i++){
            skip[i] = 0;
        }

        for(int i = args->k; i < args->k + args->m; i++){
            skip[i] = 1;
        }

        num_avail_frags = create_frags_array(&avail_frags, encoded_data, encoded_parity, args, skip);
        assert(num_avail_frags > 0);
        rc = liberasurecode_decode(desc, avail_frags, num_avail_frags, chunks->at(0)->size(), 1, &decoded_data,
                                   &decoded_data_len);
        if(rc != 0){
            DPRINTF(DEBUG_CAS_Client, "rc is %d.\n", rc);
        }

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

namespace CAS_helper_recon{

    void _send_one_server(const std::string operation, std::promise <strVec>&& prm, const  std::string key,
                          const Server* server, const std::string current_class, const uint32_t conf_id, const std::string value = "",
                          const std::string timestamp = ""){

        DPRINTF(DEBUG_CAS_Client, "started.\n");

        strVec data;
        Connect c(server->ip, server->port);
        if(!c.is_connected()){
            prm.set_value(std::move(data));
            return;
        }

        data.push_back(operation); // get_timestamp, put, put_fin, get
        data.push_back(key);
        if(operation == "reconfig_write" || operation == "finish_reconfig"){
            if(value.empty()){
                DPRINTF(DEBUG_CAS_Client, "WARNING!!! SENDING EMPTY STRING TO SERVER.\n");
            }
            data.push_back(timestamp);
            data.push_back(value);
        }
        else if(operation == "reconfig_finalize"){
            data.push_back(timestamp);
        }
        else if(operation == "reconfig_query"){

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

    inline uint32_t number_of_received_responses(std::vector<bool>& done){
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
    int failure_support_optimized(const std::string& operation, const std::string& key, const std::string& timestamp, const std::vector<std::string*>& values, uint32_t RAs,
                                  std::unordered_set <uint32_t> servers, uint32_t number_to_respond, std::vector<DC*>& datacenters,
                                  const std::string current_class, const uint32_t conf_id, uint32_t timeout_per_request, std::vector<strVec> &ret){
        DPRINTF(DEBUG_CAS_Client, "started.\n");

        std::map <uint32_t, std::future<strVec> > responses; // server_id, future
        std::vector<bool> done(servers.size(), false);
        ret.clear();

        int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
        for (auto it = servers.begin(); it != servers.end(); it++) { // request to all servers
            std::promise <strVec> prm;
            responses.emplace(*it, prm.get_future());
            std::thread(&_send_one_server, operation, std::move(prm), key, datacenters[*it]->servers[0],
                        current_class, conf_id, *(values[*it]), timestamp).detach();
        }

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
                    if(number_of_received_responses(done) == number_to_respond){
                        op_status = 0;
                        break;
                    }
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

        return op_status;
    }

    inline void free_chunks(std::vector<std::string*>& chunks){
        for(uint i = 0; i < chunks.size(); i++){
            delete chunks[i];
        }
        chunks.clear();
    }
}

namespace ABD_helper_recon{
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
        if(operation == "reconfig_write" || operation == "finish_reconfig"){
            if(value.empty()){
                DPRINTF(DEBUG_CAS_Client, "WARNING!!! SENDING EMPTY STRING TO SERVER.\n");
            }
            data.push_back(timestamp);
            data.push_back(value);
        }
        else if(operation == "reconfig_query"){

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
                                  std::unordered_set <uint32_t> servers, uint32_t number_to_respond, std::vector<DC*>& datacenters,
                                  const std::string current_class, const uint32_t conf_id, uint32_t timeout_per_request, std::vector<strVec> &ret){

        DPRINTF(DEBUG_CAS_Client, "started.\n");

        std::map <uint32_t, std::future<strVec> > responses; // server_id, future
        std::vector<bool> done(servers.size(), false);
        ret.clear();

        int op_status = 0;    // 0: Success, -1: timeout, -2: operation_fail(reconfiguration)
        for (auto it = servers.begin(); it != servers.end(); it++) { // request to all servers
            if (responses.find(*it) != responses.end()) {
                continue;
            }
            std::promise <strVec> prm;
            responses.emplace(*it, prm.get_future());
            std::thread(&_send_one_server, operation, std::move(prm), key, datacenters[*it]->servers[0],
                        current_class, conf_id, value, timestamp).detach();
        }

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
                    if(number_of_received_responses(done) == number_to_respond){
                        op_status = 0;
                        break;
                    }
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

        return op_status;
    }
}

Reconfig::Reconfig(uint32_t id, uint32_t local_datacenter_id, uint32_t retry_attempts, uint32_t metadata_server_timeout,
        uint32_t timeout_per_request, std::vector<DC*>& datacenters) : id(id), local_datacenter_id(local_datacenter_id),
                                                                       retry_attempts(retry_attempts),
                                                                       metadata_server_timeout(metadata_server_timeout),
                                                                       timeout_per_request(timeout_per_request),
                                                                       datacenters(datacenters){

    uint datacenter_indx = 0;
    bool datacenter_indx_found = false;
    for(; datacenter_indx < datacenters.size(); datacenter_indx++){
        if(datacenters[datacenter_indx]->id == local_datacenter_id){
            datacenter_indx_found = true;
            break;
        }
    }
    if(!datacenter_indx_found){
        DPRINTF(DEBUG_CAS_Client, "wrong local_datacenter_id %d.\n", local_datacenter_id);
    }
    assert(datacenter_indx_found);
    this->metadata_server_ip = datacenters[datacenter_indx]->metadata_server_ip;
    this->metadata_server_port = std::to_string(datacenters[datacenter_indx]->metadata_server_port);
}

Reconfig::~Reconfig(){
}

int Reconfig::update_metadata_info(std::string& key, uint32_t old_confid_id, uint32_t new_confid_id, const std::string& timestamp,
                         const Placement& p){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    for(uint k = 0; k < datacenters.size(); k++){
        Connect c(datacenters[k]->metadata_server_ip, datacenters[k]->metadata_server_port);
        if(!c.is_connected()){
            DPRINTF(DEBUG_RECONFIG_CONTROL, "Warn: cannot connect to metadata server\n");
            continue;
        }
        DataTransfer::sendMsg(*c, DataTransfer::serializeMDS("update",
                                                             key + "!" + std::to_string(old_confid_id) + "!" + std::to_string(new_confid_id) + "!" + timestamp,
                                                             &p));
        std::string recvd;
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            fflush(stdout);
            std::string status;
            std::string msg;
            DataTransfer::deserializeMDS(recvd, status, msg);
            if(status != "OK"){
                DPRINTF(DEBUG_RECONFIG_CONTROL, "%s\n", msg.c_str());
                assert(false);
            }
            DPRINTF(DEBUG_RECONFIG_CONTROL, "metadata_server updated\n");
        }
        else{
            DPRINTF(DEBUG_RECONFIG_CONTROL, "Error in receiving msg from Metadata Server\n");
            return -1;
        }
    }

    return S_OK;
}

int Reconfig::update_metadata_state(std::string& key, uint32_t old_confid_id, std::string& op, const std::string& timestamp){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    for(uint k = 0; k < datacenters.size(); k++){
        Connect c(datacenters[k]->metadata_server_ip, datacenters[k]->metadata_server_port);
        if(!c.is_connected()){
            DPRINTF(DEBUG_RECONFIG_CONTROL, "Warn: cannot connect to metadata server\n");
            continue;
        }
        DataTransfer::sendMsg(*c, DataTransfer::serializeMDS("state",
                                                             key + "!" + std::to_string(old_confid_id) + "!" + op + "!" + timestamp));
        std::string recvd;
        if(DataTransfer::recvMsg(*c, recvd) == 1){
            fflush(stdout);
            std::string status;
            std::string msg;
            DataTransfer::deserializeMDS(recvd, status, msg);
            if(status != "OK"){
                DPRINTF(DEBUG_RECONFIG_CONTROL, "%s\n", msg.c_str());
                assert(false);
            }
            DPRINTF(DEBUG_RECONFIG_CONTROL, "metadata_server state updated\n");
        }
        else{
            DPRINTF(DEBUG_RECONFIG_CONTROL, "Error in receiving msg from Metadata Server\n");
            return -1;
        }
    }

    return S_OK;
}

int Reconfig::start_reconfig(GroupConfig& old_config, uint32_t old_conf_id, GroupConfig& new_config, uint32_t new_conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    std::string& state_ready = "ready";
    std::string& state_toretire = "toretire";
    std::string& state_retired = "retired";

    for(uint i = 0; i < old_config.keys.size(); i++){
        std::string& key = old_config.keys[i];

        Timestamp* ret_ts = nullptr;
        std::string ret_v;
        assert(Reconfig::send_reconfig_query(old_config, old_conf_id, key, ret_ts, ret_v) == 0);
        if(old_config.placement_p->protocol == CAS_PROTOCOL_NAME){
            assert(Reconfig::send_reconfig_finalize(old_config, old_conf_id, key, ret_ts, ret_v) == 0);
        }
        assert(Reconfig::send_reconfig_write(new_config, new_conf_id, key, ret_ts, ret_v) == 0);

        assert(update_metadata_state(key, old_conf_id, "add", ret_ts->get_string()) == 0);
        
        assert(update_metadata_info(key, old_conf_id, new_conf_id, ret_ts->get_string(), *new_config.placement_p) == 0);

        assert(Reconfig::send_reconfig_finish(old_config, old_conf_id, new_conf_id, key, ret_ts) == 0);

        assert(update_metadata_state(key, old_conf_id, "remove", new_conf_id, ret_ts->get_string()) == 0);
        
        delete ret_ts;
    }
    return S_OK;
}

int Reconfig::send_reconfig_query(GroupConfig& old_config, uint32_t old_conf_id, const std::string& key, Timestamp*& ret_ts,
        std::string& ret_v){
    
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());

    ret_ts = nullptr;
    int op_status = 0;
    std::unordered_set <uint32_t> servers;
    set_intersection(*old_config.placement_p, servers);

    for(auto &t: servers){
        DPRINTF(DEBUG_RECONFIG_CONTROL, "s %u\n", t);
    }

    std::vector<strVec> ret;

    if(old_config.placement_p->protocol == ABD_PROTOCOL_NAME){

        std::vector <Timestamp> tss;
        std::vector <std::string> vs;
        uint32_t idx = -1;

        DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
//#ifndef No_GET_OPTIMIZED
//        if(p.Q1.size() < p.Q2.size()) {
//            op_status = ABD_helper_recon::failure_support_optimized("get", key, "", "", this->retry_attempts, p.Q2,
//                                                              servers,
//                                                              this->datacenters, old_config.placement_p->protocol,
//                                                              old_conf_id,
//                                                              this->timeout_per_request, ret);
//        }
//        else{
//            op_status = ABD_helper_recon::failure_support_optimized("get", key, "", "", this->retry_attempts, p.Q1,
//                                                              servers,
//                                                              this->datacenters, old_config.placement_p->protocol,
//                                                              old_conf_id,
//                                                              this->timeout_per_request, ret);
//        }
//#else
        op_status = ABD_helper_recon::failure_support_optimized("reconfig_query", key, "", "", this->retry_attempts, servers, servers.size() - old_config.placement_p->Q2.size() + 1,
                                                             this->datacenters, old_config.placement_p->protocol, old_conf_id,
                                                             this->timeout_per_request, ret);
//#endif

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
//            else if((*it)[0] == "operation_fail"){
//                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
//                parent->get_placement(key, true, stoul((*it)[1]));
//                op_status = -2; // reconfiguration happened on the key
//                return S_RECFG;
//                //break;
//            }
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

        ret_ts = new Timestamp(tss[idx]);
        ret_v = vs[idx];
    }
    else if(old_config.placement_p->protocol == CAS_PROTOCOL_NAME){
        std::vector <Timestamp> tss;
        std::vector<std::string*> chunks_temp;
        for (auto it = servers.begin(); it != servers.end(); it++) { // request to all servers
            chunks_temp.push_back(new std::string());
        }
        DPRINTF(DEBUG_CAS_Client, "calling failure_support_optimized.\n");
//#ifndef No_GET_OPTIMIZED
//        if(p.Q1.size() < p.Q4.size()) {
//            op_status = CAS_helper_recon::failure_support_optimized("get_timestamp", key, "", chunks_temp, this->retry_attempts, p.Q4,
//                                                              servers,
//                                                              this->datacenters, old_config.placement_p->protocol,
//                                                              old_conf_id,
//                                                              this->timeout_per_request, ret);
//        }
//        else{
//            op_status = CAS_helper_recon::failure_support_optimized("get_timestamp", key, "", chunks_temp, this->retry_attempts, p.Q1,
//                                                              servers,
//                                                              this->datacenters, old_config.placement_p->protocol,
//                                                              old_conf_id,
//                                                              this->timeout_per_request, ret);
//        }
//#else
        op_status = CAS_helper_recon::failure_support_optimized("reconfig_query", key, "", chunks_temp, this->retry_attempts, servers, max(servers.size() - old_config.placement_p->Q3.size() + 1, servers.size() - old_config.placement_p->Q4.size() + 1),
                                                             this->datacenters, old_config.placement_p->protocol, old_conf_id,
                                                             this->timeout_per_request, ret);
//#endif

        CAS_helper_recon::free_chunks(chunks_temp);

        DPRINTF(DEBUG_CAS_Client, "op_status: %d.\n", op_status);
        if(op_status == -1) {
            return op_status;
        }

        for(auto it = ret.begin(); it != ret.end(); it++) {
            if((*it)[0] == "OK"){
                tss.emplace_back((*it)[1]);

                // Todo: make sure that the statement below is ture!
                op_status = 0;   // For get_timestamp, even if one response Received operation is success
            }
//            else if((*it)[0] == "operation_fail"){
//                DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
//                parent->get_placement(key, true, stoul((*it)[1]));
//                op_status = -2; // reconfiguration happened on the key
//                timestamp = nullptr;
//                return S_RECFG;
//            }
            else{
                assert(false);
            }
            //else : The server returned "Failed", that means the entry was not found
            // We ignore the response in that case.
            // The servers can return Failed for timestamp, which is acceptable

            if(op_status == 0){
                ret_ts = new Timestamp(Timestamp::max_timestamp2(tss));

                DPRINTF(DEBUG_CAS_Client, "finished successfully. Max timestamp received is %s\n",
                        (ret_ts)->get_string().c_str());

//#ifndef No_GET_OPTIMIZED
//                // Check if Q4 responses has the max timestamp
//                uint32_t resp_counter = 0;
//                for(uint32_t i = 0; i < tss.size(); i++) {
//                    if(tss[i] == *timestamp)
//                        resp_counter++;
//                }
//
//                if(resp_counter >= p.Q4.size()){
//                    can_be_optimized = true;
//                }
//#endif
            }
            else{
                DPRINTF(DEBUG_CAS_Client, "Operation Failed. op_status is %d\n", op_status);
                fflush(stdout);
                assert(false);
                return S_FAIL;
            }

        }
    }
    else{
        assert(false); // Unknown protocol
    }

    return op_status;
}

int Reconfig::send_reconfig_finalize(GroupConfig& old_config, uint32_t old_conf_id, const std::string& key, Timestamp* const & ts,
        std::string& ret_v){
    
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());

    bool uninitialized_key = false;
    int op_status = 0;
    std::unordered_set <uint32_t> servers;
    set_intersection(*old_config.placement_p, servers);
    std::vector<strVec> ret;
    std::vector<std::string*> chunks;
    std::vector<std::string*> chunks_temp;
    for (auto it = servers.begin(); it != servers.end(); it++) { // request to all servers
        chunks_temp.push_back(new std::string());
    }
    op_status = CAS_helper_recon::failure_support_optimized("reconfig_finalize", key, ts->get_string(), chunks_temp, this->retry_attempts,
                                                      servers, old_config.placement_p->Q4.size(), this->datacenters, old_config.placement_p->protocol,
                                                      old_conf_id,
                                                      this->timeout_per_request, ret);

    DPRINTF(DEBUG_CAS_Client, "op_status: %d.\n", op_status);
    if(op_status == -1) {
        return op_status;
    }

    CAS_helper_recon::free_chunks(chunks_temp);

    for(auto it = ret.begin(); it != ret.end(); it++) {
        if((*it)[0] == "OK"){
            if((*it)[1] == "Ack"){

            }
            else if((*it)[1] == "init"){
                uninitialized_key = true;
            }
            else{
                chunks.push_back(new std::string((*it)[1]));
            }
        }
//        else if((*it)[0] == "operation_fail"){
//            DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
//            parent->get_placement(key, true, stoul((*it)[1]));
//            op_status = -2; // reconfiguration happened on the key
//            CAS_helper_recon::free_chunks(chunks);
//            return S_RECFG;
//        }
        else{
            DPRINTF(DEBUG_CAS_Client, "wrong message received: %s : %s\n", (*it)[0].c_str(), (*it)[1].c_str());
            fflush(stdout);
            CAS_helper_recon::free_chunks(chunks);
            assert(false);
            op_status = -8; // Bad message received from server
            return -8;
        }
    }

    if(!uninitialized_key){
        if(chunks.size() < old_config.placement_p->k){
            CAS_helper_recon::free_chunks(chunks);
            op_status = -9;
            DPRINTF(DEBUG_CAS_Client, "chunks.size() < p.k key : %s\n", key.c_str());
        }

        if(op_status != 0){
            DPRINTF(DEBUG_CAS_Client, "get operation failed for key: %s op_status is %d\n", key.c_str(), op_status);
            CAS_helper_recon::free_chunks(chunks);
            return op_status;
        }

        // Decode only if above operation success
        struct ec_args null_args;
        null_args.k = old_config.placement_p->k;
        null_args.m = old_config.placement_p->m - old_config.placement_p->k;
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

        int desc = create_liberasure_instance(old_config.placement_p);

        liberasure_recon::decode(&ret_v, &chunks, &null_args, desc);

        destroy_liberasure_instance(desc);
    }
    else{
        if(op_status != 0){
            DPRINTF(DEBUG_CAS_Client, "get operation failed for key: %s op_status is %d\n", key.c_str(), op_status);
            CAS_helper_recon::free_chunks(chunks);
            return op_status;
        }
        else{
            ret_v = "__Uninitiliazed";
        }
    }

//    DPRINTF(DEBUG_CAS_Client, "latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

    CAS_helper_recon::free_chunks(chunks);

    return op_status;
}

int Reconfig::send_reconfig_write(GroupConfig& new_config, uint32_t new_conf_id, const std::string& key, Timestamp* const & ts,
        const std::string& value){
    
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());

    int op_status = 0;
    std::unordered_set <uint32_t> servers;
    set_intersection(*new_config.placement_p, servers);
    std::vector<strVec> ret;

    if(new_config.placement_p->protocol == ABD_PROTOCOL_NAME){
        DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
        op_status = ABD_helper_recon::failure_support_optimized("reconfig_write", key, ts->get_string(), value, this->retry_attempts, servers,
                                                          new_config.placement_p->Q2.size(), this->datacenters, new_config.placement_p->protocol, new_conf_id,
                                                          this->timeout_per_request, ret);

        DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
        if(op_status == -1) {
            return op_status;
        }

        for(auto it = ret.begin(); it != ret.end(); it++) {

            if((*it)[0] == "OK"){
                DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
            }
//            else if((*it)[0] == "operation_fail"){
//                if(timestamp != nullptr){
//                    delete timestamp;
//                    timestamp = nullptr;
//                }
//                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
//                parent->get_placement(key, true, stoul((*it)[1]));
////            assert(p != nullptr);
//                op_status = -2; // reconfiguration happened on the key
//                return S_RECFG;
//            }
            else{
                DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
                return -3; // Bad message received from server
            }
        }

//        DPRINTF(DEBUG_CAS_Client, "put latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

        if(op_status != 0){
            DPRINTF(DEBUG_ABD_Client, "pre_write could not succeed\n");
            return -4; // pre_write could not succeed.
        }
    }
    else if(new_config.placement_p->protocol == CAS_PROTOCOL_NAME){
        std::vector <std::string*> chunks;
        struct ec_args null_args;
        null_args.k = new_config.placement_p->k;
        null_args.m = new_config.placement_p->m - new_config.placement_p->k; // m here is the number of parity chunks
        null_args.w = 16;
        null_args.ct = CHKSUM_NONE;
        int desc = create_liberasure_instance(new_config.placement_p);

        liberasure_recon::encode(&value, &chunks, &null_args, desc);

        destroy_liberasure_instance(desc);

        DPRINTF(DEBUG_CAS_Client, "calling failure_support_optimized.\n");
        op_status = CAS_helper_recon::failure_support_optimized("reconfig_write", key, ts->get_string(), chunks, this->retry_attempts, servers,
                                                          max(new_config.placement_p->Q2.size(), new_config.placement_p->Q3.size()), this->datacenters,
                                                          new_config.placement_p->protocol, new_conf_id,
                                                          this->timeout_per_request, ret);

        DPRINTF(DEBUG_CAS_Client, "op_status: %d.\n", op_status);
        if(op_status == -1) {
            return op_status;
        }

        for(auto it = ret.begin(); it != ret.end(); it++) {
            if((*it)[0] == "OK"){
                DPRINTF(DEBUG_CAS_Client, "OK received for key : %s\n", key.c_str());
            }
//            else if((*it)[0] == "operation_fail"){
//                DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
//                parent->get_placement(key, true, stoul((*it)[1]));
//                op_status = -2; // reconfiguration happened on the key
//                CAS_helper::free_chunks(chunks);
//                if(timestamp != nullptr){
//                    delete timestamp;
//                    timestamp = nullptr;
//                }
//                return S_RECFG;
//            }
            else{
                DPRINTF(DEBUG_CAS_Client, "Bad message received from server for key : %s\n", key.c_str());
                CAS_helper_recon::free_chunks(chunks);
                return -3; // Bad message received from server
            }
        }

//        DPRINTF(DEBUG_CAS_Client, "latencies%d:pre %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

        CAS_helper_recon::free_chunks(chunks);
        if(op_status != 0){
            DPRINTF(DEBUG_CAS_Client, "pre_write could not succeed. op_status is %d\n", op_status);
            return -4; // pre_write could not succeed.
        }
    }
    else{
        assert(false); // Unknown protocol
    }

    return op_status;
}

int Reconfig::send_reconfig_finish(GroupConfig& old_config, uint32_t old_conf_id, uint32_t new_conf_id, const std::string& key,
        Timestamp* const & ts){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started on key \"%s\"\n", key.c_str());

    int op_status = 0;
    std::unordered_set <uint32_t> servers;
    set_intersection(*old_config.placement_p, servers);
    std::vector<strVec> ret;

    if(old_config.placement_p->protocol == ABD_PROTOCOL_NAME){
        DPRINTF(DEBUG_ABD_Client, "calling failure_support_optimized.\n");
        op_status = ABD_helper_recon::failure_support_optimized("finish_reconfig", key, ts->get_string(), std::to_string(new_conf_id), this->retry_attempts, servers,
                                                                servers.size() - old_config.placement_p->f, this->datacenters, old_config.placement_p->protocol, old_conf_id,
                                                                this->timeout_per_request, ret);

        DPRINTF(DEBUG_ABD_Client, "op_status: %d.\n", op_status);
        if(op_status == -1) {
            return op_status;
        }

        for(auto it = ret.begin(); it != ret.end(); it++) {

            if((*it)[0] == "OK"){
                DPRINTF(DEBUG_ABD_Client, "OK received for key : %s\n", key.c_str());
            }
//            else if((*it)[0] == "operation_fail"){
//                if(timestamp != nullptr){
//                    delete timestamp;
//                    timestamp = nullptr;
//                }
//                DPRINTF(DEBUG_ABD_Client, "operation_fail received for key : %s\n", key.c_str());
//                parent->get_placement(key, true, stoul((*it)[1]));
////            assert(p != nullptr);
//                op_status = -2; // reconfiguration happened on the key
//                return S_RECFG;
//            }
            else{
                DPRINTF(DEBUG_ABD_Client, "Bad message received from server for key : %s\n", key.c_str());
                return -3; // Bad message received from server
            }
        }

//        DPRINTF(DEBUG_CAS_Client, "put latencies%d: %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

        if(op_status != 0){
            DPRINTF(DEBUG_ABD_Client, "pre_write could not succeed\n");
            return -4; // pre_write could not succeed.
        }
    }
    else if(old_config.placement_p->protocol == CAS_PROTOCOL_NAME){
        std::vector<std::string*> chunks_temp;
        for (auto it = servers.begin(); it != servers.end(); it++) { // request to all servers
            chunks_temp.push_back(new std::string(std::to_string(new_conf_id)));
        }
        DPRINTF(DEBUG_CAS_Client, "calling failure_support_optimized.\n");
        op_status = CAS_helper_recon::failure_support_optimized("finish_reconfig", key, ts->get_string(), chunks_temp, this->retry_attempts, servers,
                                                                servers.size() - old_config.placement_p->f, this->datacenters,
                                                                old_config.placement_p->protocol, old_conf_id,
                                                                this->timeout_per_request, ret);

        DPRINTF(DEBUG_CAS_Client, "op_status: %d.\n", op_status);
        if(op_status == -1) {
            return op_status;
        }
        CAS_helper_recon::free_chunks(chunks_temp);
        for(auto it = ret.begin(); it != ret.end(); it++) {
            if((*it)[0] == "OK"){
                DPRINTF(DEBUG_CAS_Client, "OK received for key : %s\n", key.c_str());
            }
//            else if((*it)[0] == "operation_fail"){
//                DPRINTF(DEBUG_CAS_Client, "operation_fail received for key : %s\n", key.c_str());
//                parent->get_placement(key, true, stoul((*it)[1]));
//                op_status = -2; // reconfiguration happened on the key
//                CAS_helper::free_chunks(chunks);
//                if(timestamp != nullptr){
//                    delete timestamp;
//                    timestamp = nullptr;
//                }
//                return S_RECFG;
//            }
            else{
                DPRINTF(DEBUG_CAS_Client, "Bad message received from server for key : %s\n", key.c_str());
                return -3; // Bad message received from server
            }
        }

//        DPRINTF(DEBUG_CAS_Client, "latencies%d:pre %lu\n", le_counter++, time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count() - le_init);

        if(op_status != 0){
            DPRINTF(DEBUG_CAS_Client, "pre_write could not succeed. op_status is %d\n", op_status);
            return -4; // pre_write could not succeed.
        }
    }
    else{
        assert(false); // Unknown protocol
    }

    return op_status;
}
