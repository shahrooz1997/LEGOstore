#include "../inc/Data_Server.h"


DataServer::DataServer(std::string directory, int sock, std::string metadata_server_ip,
        std::string metadata_server_port) : sockfd(sock), cache(1000000000), persistent(directory),
        CAS(&recon_keys, &metadata_server_ip, &metadata_server_port),
        ABD(&recon_keys, &metadata_server_ip, &metadata_server_port){
    this->metadata_server_port = metadata_server_port;
    this->metadata_server_ip = metadata_server_ip;
}

void DataServer::check_block_keys(std::string& key, std::string& curr_class, uint32_t conf_id){
    std::unique_lock<std::mutex> lock(this->blocked_keys_lock);

    // See if the key is in block mode
    std::string con_key = construct_key(key, curr_class, conf_id);
    while(std::find(this->blocked_keys.begin(), this->blocked_keys.end(), con_key) != this->blocked_keys.end()){
        this->blocked_keys_cv.wait(lock);
    }

    return;
}

void DataServer::add_block_keys(std::string& key, std::string& curr_class, uint32_t conf_id){
    std::unique_lock<std::mutex> lock(this->blocked_keys_lock);

    std::string con_key = construct_key(key, curr_class, conf_id);
    assert(std::find(this->blocked_keys.begin(), this->blocked_keys.end(), con_key) == this->blocked_keys.end());
    this->blocked_keys.push_back(con_key);

    return;
}

void DataServer::remove_block_keys(std::string& key, std::string& curr_class, uint32_t conf_id){
    std::unique_lock<std::mutex> lock(this->blocked_keys_lock);

    std::string con_key = construct_key(key, curr_class, conf_id);
    auto it = std::find(this->blocked_keys.begin(), this->blocked_keys.end(), con_key);
    assert(it != this->blocked_keys.end());
    this->blocked_keys.erase(it);

    return;
}

int DataServer::getSocketDesc(){
    return sockfd;
}

std::string DataServer::get_timestamp(std::string& key, std::string& curr_class, uint32_t conf_id, const Request& req){

    check_block_keys(key, curr_class, conf_id);

    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.get_timestamp(key, conf_id, req, cache, persistent, lock);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.get_timestamp(key, conf_id, req, cache, persistent, lock);
    }
    
    return result;
}

std::string DataServer::reconfig_query(std::string &key, std::string &curr_class, uint32_t conf_id){

    add_block_keys(key, curr_class, conf_id);

    std::string result;
    Request req; // ToDo: we do not need this variable anymore. Remove it from all the functions
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.get_timestamp(key, conf_id, req, cache, persistent, lock);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        std::string phony_timestamp = "";
        result = ABD.get(key, conf_id, phony_timestamp, req, cache, persistent, lock);
    }

    return result;
}

//std::string DataServer::reconfig_finalize(std::string &key, std::string &timestamp, std::string &curr_class, uint32_t conf_id){
//    std::string result;
//    if(curr_class == CAS_PROTOCOL_NAME){
//        result = CAS.get(key, conf_id, timestamp, req, cache, persistent, lock);
//    }else{
//        //Error scenario;
//    }
//    return result;
//}
//
//std::string DataServer::write_config(std::string &key, std::string &value, std::string &timestamp, std::string &curr_class, uint32_t conf_id){
//
//    std::string result;
//    if(curr_class == CAS_PROTOCOL_NAME){
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
//        result = CAS.put(key, conf_id, value, timestamp, req, cache, persistent, lock);
//        result = CAS.put_fin(key, conf_id, timestamp, req, cache, persistent, lock);
//    }else if(curr_class == ABD_PROTOCOL_NAME){
//        result = ABD.put(key, conf_id, value, timestamp, cache, persistent, lock);
//    }
//
//    return result;
//}

std::string
DataServer::put(std::string& key, std::string& value, std::string& timestamp, std::string& curr_class, uint32_t conf_id,
        const Request& req){

    check_block_keys(key, curr_class, conf_id);

    std::string result;
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
        result = CAS.put(key, conf_id, value, timestamp, req, cache, persistent, lock);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.put(key, conf_id, value, timestamp, req, cache, persistent, lock);
    }
    return result;
}

std::string DataServer::put_fin(std::string& key, std::string& timestamp, std::string& curr_class, uint32_t conf_id,
        const Request& req){

    check_block_keys(key, curr_class, conf_id);

    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.put_fin(key, conf_id, timestamp, req, cache, persistent, lock);
    }
    // else if(curr_class == ABD_PROTOCOL_NAME){
    //
    // }
    return result;
}

std::string DataServer::get(std::string& key, std::string& timestamp, std::string& curr_class, uint32_t conf_id,
        const Request& req){

    check_block_keys(key, curr_class, conf_id);

    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.get(key, conf_id, timestamp, req, cache, persistent, lock);
    }
    else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.get(key, conf_id, timestamp, req, cache, persistent, lock);
    }
    return result;
}
