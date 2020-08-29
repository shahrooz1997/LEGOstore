#include "Data_Server.h"

int DataServer::getSocketDesc(){
    return sockfd;
}

std::string DataServer::get_timestamp(std::string &key, std::string &curr_class, uint32_t conf_id, const Request& req){

    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.get_timestamp(key, conf_id, req, cache, persistent, lock);
    }else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.get_timestamp(key, conf_id, cache, persistent, lock);
    }

    return result;
}

//std::string DataServer::reconfig_query(std::string &key, std::string &curr_class, uint32_t conf_id){
//
//    std::string result;
//    if(curr_class == CAS_PROTOCOL_NAME){
//        result = CAS.get_timestamp(key, conf_id, req, cache, persistent, lock);
//    }else if(curr_class == ABD_PROTOCOL_NAME){
//        result = ABD.get_timestamp(key, conf_id, cache, persistent, lock);
//    }
//
//    return result;
//}
//
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

std::string DataServer::put(std::string &key, std::string &value, std::string &timestamp, std::string &curr_class, uint32_t conf_id, const Request& req){
    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        char bbuf[1024*128];
        int bbuf_i = 0;
//        for(int t = 0; t < chunks.size(); t++){
        bbuf_i += sprintf(bbuf + bbuf_i, "%s-chunk = ", key.c_str());
        for(uint tt = 0; tt < value.size(); tt++){
            bbuf_i += sprintf(bbuf + bbuf_i, "%02X", value.at(tt) & 0xff);
//                printf("%02X", chunks[t]->at(tt));
        }
        bbuf_i += sprintf(bbuf + bbuf_i, "\n");
//        }
        printf("%s", bbuf);
        result = CAS.put(key, conf_id, value, timestamp, req, cache, persistent, lock);
    }else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.put(key, conf_id, value, timestamp, cache, persistent, lock);
    }
    return result;
}

std::string DataServer::put_fin(std::string &key, std::string &timestamp, std::string &curr_class, uint32_t conf_id, const Request& req){
    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.put_fin(key, conf_id, timestamp, req, cache, persistent, lock);
    }
    // else if(curr_class == ABD_PROTOCOL_NAME){
    //
    // }
    return result;
}

std::string DataServer::get(std::string &key, std::string &timestamp, std::string &curr_class, uint32_t conf_id, const Request& req){
    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.get(key, conf_id, timestamp, req, cache, persistent, lock);
    }else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.get(key, conf_id, timestamp, cache, persistent, lock);
    }
    return result;
}
