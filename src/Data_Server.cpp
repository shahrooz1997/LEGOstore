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

void DataServer::check_block_keys(const string& key, const string& curr_class, uint32_t conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    unique_lock<mutex> lock(this->blocked_keys_lock);

    // See if the key is in block mode
    string con_key = construct_key(key, curr_class, conf_id);
    while(find(this->blocked_keys.begin(), this->blocked_keys.end(), con_key) != this->blocked_keys.end()){
        this->blocked_keys_cv.wait(lock);
    }

    return;
}

void DataServer::add_block_keys(const string& key, const string& curr_class, uint32_t conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    unique_lock<mutex> lock(this->blocked_keys_lock);

    string con_key = construct_key(key, curr_class, conf_id);
    assert(find(this->blocked_keys.begin(), this->blocked_keys.end(), con_key) == this->blocked_keys.end());
    this->blocked_keys.push_back(con_key);

    return;
}

void DataServer::remove_block_keys(const string& key, const string& curr_class, uint32_t conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    unique_lock<mutex> lock(this->blocked_keys_lock);

    string con_key = construct_key(key, curr_class, conf_id);
    auto it = find(this->blocked_keys.begin(), this->blocked_keys.end(), con_key);

    if(it == this->blocked_keys.end()){
        DPRINTF(DEBUG_RECONFIG_CONTROL, "WARN: it != this->blocked_keys.end() for con_key: %s\n", con_key.c_str());
    }
    if(it != this->blocked_keys.end()){
        this->blocked_keys.erase(it);
    }
    this->blocked_keys_cv.notify_all();
    return;
}

string DataServer::reconfig_query(const string& key, const string& curr_class, uint32_t conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    add_block_keys(key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.get_timestamp(key, conf_id);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.get(key, conf_id);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::reconfig_finalize(const string& key, const string& timestamp, const string& curr_class, uint32_t conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

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
        return ABD.put(key, conf_id, value, timestamp);
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
//    if(curr_class == CAS_PROTOCOL_NAME){
//        string con_key = construct_key(key, CAS_PROTOCOL_NAME, conf_id);
//        strVec data = get_data(con_key);
//        if(data.empty()){
//            CAS.init_key(key, conf_id);
//            data = get_data(con_key);
//        }
//        data[1] = timestamp;
//        data[2] = new_conf_id;
//        put_data(con_key, data);
//    }
//    else if(curr_class == ABD_PROTOCOL_NAME){
//        string con_key = construct_key(key, ABD_PROTOCOL_NAME, conf_id);
//        strVec data = get_data(con_key);
//        if(data.empty()){
//            ABD.init_key(key, conf_id);
//            data = get_data(con_key);
//        }
//        data[3] = timestamp;
//        data[4] = new_conf_id;
//        put_data(con_key, data);
//    }
//    else{
//        assert(false);
//    }
    lock.unlock();

    remove_block_keys(key, curr_class, conf_id);

    return DataTransfer::serialize({"OK"});
}

int DataServer::getSocketDesc(){
    return sockfd;
}

string DataServer::get_timestamp(const string& key, const string& curr_class, uint32_t conf_id){

    check_block_keys(key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.get_timestamp(key, conf_id);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.get_timestamp(key, conf_id);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::put(const string& key, const string& value, const string& timestamp, const string& curr_class, uint32_t conf_id){

    check_block_keys(key, curr_class, conf_id);

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
        return ABD.put(key, conf_id, value, timestamp);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::put_fin(const string& key, const string& timestamp, const string& curr_class, uint32_t conf_id){

    check_block_keys(key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.put_fin(key, conf_id, timestamp);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

string DataServer::get(const string& key, const string& timestamp, const string& curr_class, uint32_t conf_id){

    check_block_keys(key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.get(key, conf_id, timestamp);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        return ABD.get(key, conf_id);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

bool Persistent_merge::Merge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
                                 const rocksdb::Slice& value, std::string* new_value, rocksdb::Logger* logger) const {

    vector<string> value_vec;
    value_vec = DataTransfer::deserialize(string(value.data(), value.size()));
    assert(!value_vec.empty());
    string operation = value_vec[0];
    value_vec.erase(value_vec.begin());

    if(operation == "ABD_Server::get_timestamp"){
        if(value_vec[0] == "init"){
            if(!existing_value){
                *new_value = DataTransfer::serialize(value_vec);
                return true;
            }
            *new_value = string(existing_value->data(), existing_value->size());
            return true;
        }
        assert(false);
    }
    else if(operation == "ABD_Server::put"){
        if(!existing_value){
            DPRINTF(DEBUG_ABD_Server, "put new con_key which is %s\n", key.data());
            vector<string> val{value_vec[0], value_vec[1], value_vec[2], "", ""};
            *new_value = DataTransfer::serialize(val);
            return true;
        }
        else{
            vector<string> data = DataTransfer::deserialize(string(existing_value->data(), existing_value->size()));
            if(data[3] == ""){
                if(Timestamp(value_vec[2]) > Timestamp(data[2])){
                    data[0] = value_vec[0];
                    data[2] = value_vec[2];
                    *new_value = DataTransfer::serialize(data);
                    return true;
                }
                *new_value = string(existing_value->data(), existing_value->size());
                return true;
            }
            else{ // Key reconfigured
                if(!(Timestamp(value_vec[2]) > Timestamp(data[3]))){
                    if(Timestamp(value_vec[2]) > Timestamp(data[2])){
                        data[0] = value_vec[0];
                        data[2] = value_vec[2];
                        *new_value = DataTransfer::serialize(data);
                        return true;
                    }
                }
                *new_value = string(existing_value->data(), existing_value->size());
                return true;
            }
        }
    }
    else if(operation == "ABD_Server::get"){
        if(value_vec[0] == "init"){
            if(!existing_value){
                *new_value = DataTransfer::serialize(value_vec);
                return true;
            }
            *new_value = string(existing_value->data(), existing_value->size());
            return true;
        }
        assert(false);
    }
    else if(operation == "CAS_Server::get_timestamp_val"){
        if(value_vec[0] == "init"){
            if(!existing_value){
                *new_value = DataTransfer::serialize(value_vec);
                return true;
            }
            *new_value = string(existing_value->data(), existing_value->size());
            return true;
        }
        assert(false);
    }
    else if(operation == "CAS_Server::get_timestamp_ts"){
        if(!existing_value){
            *new_value = DataTransfer::serialize(value_vec);
            return true;
        }
        *new_value = string(existing_value->data(), existing_value->size());
        return true;
    }
    else if(operation == "CAS_Server::put"){
        if(!existing_value){
            DPRINTF(DEBUG_ABD_Server, "put new con_key which is %s\n", key.data());
            *new_value = DataTransfer::serialize(value_vec);
            return true;
        }
        else{
            *new_value = string(existing_value->data(), existing_value->size());
            DPRINTF(DEBUG_ABD_Server, "repetitive put message for key %s\n", key.data());
//            assert(false);
            return true;
        }
    }
    else if(operation == "CAS_Server::put_fin_val"){
        if(!existing_value){
            DPRINTF(DEBUG_ABD_Server, "put new con_key which is %s\n", key.data());
            *new_value = DataTransfer::serialize(value_vec);
            return true;
        }
        else{
            *new_value = string(existing_value->data(), existing_value->size());
            return true;
        }
    }
    else if(operation == "CAS_Server::put_fin_ts"){
        if(!existing_value){
            assert(false);
        }
        else{
            vector<string> data = DataTransfer::deserialize(string(existing_value->data(), existing_value->size()));
            if(data[1] == ""){
                if(Timestamp(value_vec[0]) > Timestamp(data[0])){
                    data[0] = value_vec[0];
                    *new_value = DataTransfer::serialize(data);
                    return true;
                }
                *new_value = string(existing_value->data(), existing_value->size());
                return true;
            }
            else{ // Key reconfigured
                if(!(Timestamp(value_vec[0]) > Timestamp(data[1]))){
                    if(Timestamp(value_vec[0]) > Timestamp(data[0])){
                        data[0] = value_vec[0];
                        *new_value = DataTransfer::serialize(data);
                        return true;
                    }
                }
                *new_value = string(existing_value->data(), existing_value->size());
                return true;
            }
        }
    }
    else if(operation == "CAS_Server::get_val"){
        if(!existing_value){
            DPRINTF(DEBUG_ABD_Server, "put new con_key which is %s\n", key.data());
            *new_value = DataTransfer::serialize(value_vec);
            return true;
        }
        else{
            *new_value = string(existing_value->data(), existing_value->size());
            return true;
        }
    }
    else if(operation == "CAS_Server::get_ts"){
        if(!existing_value){
            assert(false);
        }
        else{
            vector<string> data = DataTransfer::deserialize(string(existing_value->data(), existing_value->size()));
            if(data[1] == ""){
                if(Timestamp(value_vec[0]) > Timestamp(data[0])){
                    data[0] = value_vec[0];
                    *new_value = DataTransfer::serialize(data);
                    return true;
                }
                *new_value = string(existing_value->data(), existing_value->size());
                return true;
            }
            else{ // Key reconfigured
                if(!(Timestamp(value_vec[0]) > Timestamp(data[1]))){
                    if(Timestamp(value_vec[0]) > Timestamp(data[0])){
                        data[0] = value_vec[0];
                        *new_value = DataTransfer::serialize(data);
                        return true;
                    }
                }
                *new_value = string(existing_value->data(), existing_value->size());
                return true;
            }
        }
    }
    else{
        DPRINTF(DEBUG_ABD_Server, "Operation was not found: %s\n", operation.c_str());
        assert(false);
    }
}
