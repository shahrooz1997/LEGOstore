#include "../inc/Data_Server.h"

using std::string;
using std::vector;
using std::shared_ptr;
using std::mutex;
using std::unique_lock;
using std::find;

DataServer::DataServer(std::string directory, int sock, std::string metadata_server_ip,
        std::string metadata_server_port) : sockfd(sock), cache(1000000000), persistent(directory),
        CAS(shared_ptr<Cache>(&cache), shared_ptr<Persistent>(&persistent), shared_ptr<mutex>(&mu)),
        ABD(shared_ptr<Cache>(&cache), shared_ptr<Persistent>(&persistent), shared_ptr<mutex>(&mu)){
    this->metadata_server_port = metadata_server_port;
    this->metadata_server_ip = metadata_server_ip;
}

strVec DataServer::get_data(const std::string& key){
    const strVec* ptr = cache.get(key);
    if(ptr == nullptr){ // Data is not in cache
        return persistent.get(key);
    }
    else{ // data found in cache
        return *ptr;
    }
}

int DataServer::put_data(const std::string& key, const strVec& value){
    cache.put(key, value);
    persistent.put(key, value);
    return S_OK;
}

void DataServer::check_block_keys(const std::string& key, const std::string& curr_class, uint32_t conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    std::unique_lock<std::mutex> lock(this->blocked_keys_lock);

    // See if the key is in block mode
    string con_key = construct_key(key, curr_class, conf_id);
    while(find(this->blocked_keys.begin(), this->blocked_keys.end(), con_key) != this->blocked_keys.end()){
        this->blocked_keys_cv.wait(lock);
    }

    return;
}

void DataServer::add_block_keys(const std::string& key, const std::string& curr_class, uint32_t conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    std::unique_lock<std::mutex> lock(this->blocked_keys_lock);

    std::string con_key = construct_key(key, curr_class, conf_id);
    assert(std::find(this->blocked_keys.begin(), this->blocked_keys.end(), con_key) == this->blocked_keys.end());
    this->blocked_keys.push_back(con_key);

    return;
}

void DataServer::remove_block_keys(const std::string& key, const std::string& curr_class, uint32_t conf_id){
    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");
    std::unique_lock<std::mutex> lock(this->blocked_keys_lock);

    std::string con_key = construct_key(key, curr_class, conf_id);
    auto it = std::find(this->blocked_keys.begin(), this->blocked_keys.end(), con_key);

    if(it == this->blocked_keys.end()){
        DPRINTF(DEBUG_RECONFIG_CONTROL, "WARN: it != this->blocked_keys.end() for con_key: %s\n", con_key.c_str());
    }
    if(it != this->blocked_keys.end()){
        this->blocked_keys.erase(it);
    }
    this->blocked_keys_cv.notify_all();
    return;
}

std::string DataServer::reconfig_query(const std::string& key, const std::string& curr_class, uint32_t conf_id){

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

std::string DataServer::reconfig_finalize(const std::string& key, const std::string& timestamp, const std::string& curr_class, uint32_t conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.get(key, conf_id, timestamp);
    }else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

std::string DataServer::reconfig_write(const std::string& key, const std::string& value, const std::string& timestamp, const std::string& curr_class,
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

std::string DataServer::finish_reconfig(const std::string &key, const std::string &timestamp, const std::string& new_conf_id, const std::string &curr_class, uint32_t conf_id){

    DPRINTF(DEBUG_RECONFIG_CONTROL, "started\n");

    //Todo: add the reconfigured timestamp to the persistent storage here.
    std::unique_lock<std::mutex> lock(mu);
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

    remove_block_keys(key, curr_class, conf_id);

    return DataTransfer::serialize({"OK"});
}

int DataServer::getSocketDesc(){
    return sockfd;
}

std::string DataServer::get_timestamp(const std::string& key, const std::string& curr_class, uint32_t conf_id){

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

std::string DataServer::put(const std::string& key, const std::string& value, const std::string& timestamp, const std::string& curr_class, uint32_t conf_id){

    check_block_keys(key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        DPRINTF(DEBUG_CAS_Client, "ddddd\n");
        fflush(stdout);
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

std::string DataServer::put_fin(const std::string& key, const std::string& timestamp, const std::string& curr_class, uint32_t conf_id){

    check_block_keys(key, curr_class, conf_id);

    if(curr_class == CAS_PROTOCOL_NAME){
        return CAS.put_fin(key, conf_id, timestamp);
    }
    else{
        assert(false);
    }
    return DataTransfer::serialize({"ERROR", "INTERNAL ERROR"});
}

std::string DataServer::get(const std::string& key, const std::string& timestamp, const std::string& curr_class, uint32_t conf_id){

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
