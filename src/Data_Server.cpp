#include "../inc/Data_Server.h"

using std::string;
using std::vector;
using std::shared_ptr;
using std::mutex;
using std::unique_lock;
using std::find;

DataServer::DataServer(string directory, int sock, string metadata_server_ip,
        string metadata_server_port) : sockfd(sock), cache(1000000000), persistent(directory),
        CAS(shared_ptr<Cache>(&cache), shared_ptr<Persistent>(&persistent), shared_ptr<mutex>(&mu)),
        ABD(shared_ptr<Cache>(&cache), shared_ptr<Persistent>(&persistent), shared_ptr<mutex>(&mu)){
    DPRINTF(true, "DATASERVER constructor\n");
    this->metadata_server_port = metadata_server_port;
    this->metadata_server_ip = metadata_server_ip;
}

strVec DataServer::get_data(const string& key){
    const strVec* ptr = cache.get(key);
    if(ptr == nullptr){ // Data is not in cache
        strVec value = persistent.get(key);
        cache.put(key, value);
        return value;
    }
    else{ // data found in cache
        return *ptr;
    }
}

int DataServer::put_data(const string& key, const strVec& value){
    cache.put(key, value);
    persistent.put(key, value);
    return S_OK;
}

bool DataServer::check_block_keys(std::vector<std::string>& blocked_keys, const std::string& key, 
        const std::string& curr_class, uint32_t conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

    // See if the key is in block mode
    string con_key = construct_key(key, curr_class, conf_id);
    if(find(blocked_keys.begin(), blocked_keys.end(), con_key) != blocked_keys.end()){
        return true;
    }

    return false;
}

void DataServer::add_block_keys(std::vector<std::string>& blocked_keys, const std::string& key,
            const std::string& curr_class, uint32_t conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

    std::string con_key = construct_key(key, curr_class, conf_id);
    assert(std::find(blocked_keys.begin(), blocked_keys.end(), con_key) == blocked_keys.end());
    blocked_keys.push_back(con_key);

    return;
}

void DataServer::remove_block_keys(std::vector<std::string>& blocked_keys, const std::string& key,
            const std::string& curr_class, uint32_t conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

    std::string con_key = construct_key(key, curr_class, conf_id);
    auto it = std::find(blocked_keys.begin(), blocked_keys.end(), con_key);

    if(it == blocked_keys.end()){
        DPRINTF(DEBUG_RECONFIG_CONTROL, "WARN: it != this->blocked_keys.end() for con_key: %s\n", con_key.c_str());
    }
    if(it != blocked_keys.end()){
        blocked_keys.erase(it);
    }
    return;
}

string DataServer::reconfig_query(const string& key, const string& curr_class, uint32_t conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    add_block_keys(this->blocked_keys, key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.get_timestamp(key, conf_id);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.get(key, conf_id, "");
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::reconfig_finalize(const string& key, const string& timestamp, const string& curr_class, uint32_t conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    add_block_keys(this->finished_reconfig_keys, key, curr_class, conf_id);
    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.get(key, conf_id, timestamp);
    }else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::reconfig_write(const string& key, const string& value, const string& timestamp, const string& curr_class,
                                       uint32_t conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

    if(curr_class == CAS_PROTOCOL_NAME){
//        char bbuf[1024*128];
//        int bbuf_i = 0;
////        for(int t = 0; t < chunks.size(); t++){
//        bbuf_i += sprintf(bbuf + bbuf_i, "write_config-%s-chunk = ", key.c_str());
//        for(uint tt = 0; tt < value.size(); tt++){
//            bbuf_i += sprintf(bbuf + bbuf_i, "%02X", value.at(tt) & 0xff);
////                printf("%02X", chunks[t]->at(tt));
//        }
//        bbuf_i += sprintf(bbuf + bbuf_i, "\n");
////        }
//        printf("%s", bbuf);

        CAS.put(key, conf_id, value, timestamp);
        return CAS.put_fin(key, conf_id, timestamp);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.put(key, conf_id, value, timestamp, "");
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::finish_reconfig(const string &key, const string &timestamp, const string& new_conf_id, const string &curr_class, uint32_t conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

    //Todo: add the reconfigured timestamp to the persistent storage here.
    unique_lock<mutex> lock(mu);
    if(curr_class == CAS_PROTOCOL_NAME){
        string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
        strVec data = get_data(con_key);
        if(data.empty()){
            CAS.init_key(key, conf_id);
            data = get_data(con_key);
        }
        data[1] = timestamp;
        data[2] = new_conf_id;
        put_data(con_key, data);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id);
        strVec data = get_data(con_key);
        if(data.empty()){
            ABD.init_key(key, conf_id);
            data = get_data(con_key);
        }
        data[3] = timestamp;
        data[4] = new_conf_id;
        put_data(con_key, data);
    }
    else{
        assert(false);
    }
    lock.unlock();

    remove_block_keys(this->blocked_keys, key, curr_class, conf_id);
    remove_block_keys(this->finished_reconfig_keys, key, curr_class, conf_id);

    return DataTransfer::serialize({"OK"});
}

int DataServer::getSocketDesc(){
    return sockfd;
}

string DataServer::get_timestamp(const string& key, const string& curr_class, uint32_t conf_id){

    bool reconfigFinished = check_block_keys(this->finished_reconfig_keys, key, curr_class, conf_id);
    bool reconfigInProgress = check_block_keys(this->blocked_keys, key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.get_timestamp(key, conf_id);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        if(reconfigFinished) {
            strVec data = get_data(key);
            return DataTransfer::serialize({"operation_fail", data[4]});
        } else if (reconfigInProgress){
            strVec data = get_data(key);
            string extra_configs = "extra_configs";
            string tofind = key + "!" + curr_class;
            for(auto it = this->blocked_keys.begin(); it != this->blocked_keys.end(); it++){
                string constructed_key = *it;
                if(constructed_key.find(tofind) != std::string::npos) {
                    size_t  pos = 0;
                    int cnt = 0;
                    while(cnt != 2) {
                        pos = constructed_key.find(tofind);
                        constructed_key.erase(0, pos + 1);
                        cnt++;
                    }
                    pos = constructed_key.find(tofind);
                    string extracted_config = constructed_key.substr(0, pos);
                    if(std::to_string(conf_id) != extracted_config) extra_configs += "!" + extracted_config;
                }
            }
            return ABD.get_timestamp(key, conf_id, extra_configs);
        } else {
            return ABD.get_timestamp(key, conf_id, "");
        }
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::put(const string& key, const string& value, const string& timestamp, const string& curr_class, uint32_t conf_id){

    bool reconfigInProgress = check_block_keys(this->blocked_keys, key, curr_class, conf_id);
    if(curr_class == CAS_PROTOCOL_NAME){
        DPRINTF(DEBUG_CAS_Client, "ddddd\n");
//        char bbuf[1024*128];
//        int bbuf_i = 0;
////        for(int t = 0; t < chunks.size(); t++){
//        bbuf_i += sprintf(bbuf + bbuf_i, "%s-chunk = ", key.c_str());
//        for(uint tt = 0; tt < value.size(); tt++){
//            bbuf_i += sprintf(bbuf + bbuf_i, "%02X", value.at(tt) & 0xff);
////                printf("%02X", chunks[t]->at(tt));
//        }
//        bbuf_i += sprintf(bbuf + bbuf_i, "\n");
////        }
//        printf("%s", bbuf);
        return CAS.put(key, conf_id, value, timestamp);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        if (reconfigInProgress){
            strVec data = get_data(key);
            string extra_configs = "extra_configs";
            string tofind = key + "!" + curr_class;
            for(auto it = this->blocked_keys.begin(); it != this->blocked_keys.end(); it++){
                string constructed_key = *it;
                if(constructed_key.find(tofind) != std::string::npos) {
                    size_t  pos = 0;
                    int cnt = 0;
                    while(cnt != 2) {
                        pos = constructed_key.find(tofind);
                        constructed_key.erase(0, pos + 1);
                        cnt++;
                    }
                    pos = constructed_key.find(tofind);
                    string extracted_config = constructed_key.substr(0, pos);
                    if(std::to_string(conf_id) != extracted_config) extra_configs += "!" + extracted_config;
                }
            }
            return ABD.put(key, conf_id, value, timestamp, extra_configs);
        } else {
            return ABD.put(key, conf_id, value, timestamp, "");
        }
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::put_fin(const string& key, const string& timestamp, const string& curr_class, uint32_t conf_id){

    check_block_keys(this->blocked_keys, key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.put_fin(key, conf_id, timestamp);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::get(const string& key, const string& timestamp, const string& curr_class, uint32_t conf_id){

    bool reconfigFinished = check_block_keys(this->finished_reconfig_keys, key, curr_class, conf_id);
    bool reconfigInProgress = check_block_keys(this->blocked_keys, key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.get(key, conf_id, timestamp);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        if(reconfigFinished) {
            strVec data = get_data(key);
            return DataTransfer::serialize({"operation_fail", data[4]});
        } else if(reconfigInProgress) {
            string extra_configs = "extra_configs";
            string tofind = key + "!" + curr_class;
            for(auto it = this->blocked_keys.begin(); it != this->blocked_keys.end(); it++){
                string constructed_key = *it;
                if(constructed_key.find(tofind) != std::string::npos) {
                    size_t  pos = 0;
                    int cnt = 0;
                    while(cnt != 2) {
                        pos = constructed_key.find(tofind);
                        constructed_key.erase(0, pos + 1);
                        cnt++;
                    }
                    pos = constructed_key.find(tofind);
                    string extracted_config = constructed_key.substr(0, pos);
                    if(std::to_string(conf_id) != extracted_config) extra_configs += "!" + extracted_config;
                }
            }
            return ABD.get(key, conf_id, extra_configs);
        } else {
            return ABD.get(key, conf_id, "");
        }
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}
