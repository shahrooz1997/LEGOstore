#include "Data_Server.h"

int DataServer::getSocketDesc(){
    return sockfd;
}

std::string DataServer::get_timestamp(std::string &key, std::string &curr_class){

    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.get_timestamp(key,cache, persistent, lock);
    }else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.get_timestamp(key,cache, persistent, lock);
    }

    return result;
}

std::string DataServer::reconfig_query(std::string &key, std::string &curr_class){

    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.get_timestamp(key, cache, persistent, lock);
    }else if(curr_class == ABD_PROTOCOL_NAME){
        // Directly do a get here for ABD i think
        std::string t; // This is dummy
        result = ABD.get(key, t, cache, persistent, lock);
    }

    return result;
}

std::string DataServer::reconfig_finalize(std::string &key, std::string &timestamp, std::string &curr_class){
    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.get(key, timestamp, cache, persistent, lock);
    }else{
        //Error scenario;
    }
    return result;
}

std::string DataServer::write_config(std::string &key, std::string &value, std::string &timestamp, std::string &curr_class){

    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.put(key,value, timestamp, cache, persistent, lock);
        result = CAS.put_fin(key, timestamp, cache, persistent, lock);
    }else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.put(key, value, timestamp, cache, persistent, lock);
    }

    return result;
}

std::string DataServer::put(std::string &key, std::string &value, std::string &timestamp, std::string &curr_class){
    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.put(key,value, timestamp, cache, persistent, lock);
    }else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.put(key,value, timestamp, cache, persistent, lock);
    }
    return result;
}

std::string DataServer::put_fin(std::string &key, std::string &timestamp, std::string &curr_class){
    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.put_fin(key, timestamp, cache, persistent, lock);
    }
    // else if(curr_class == ABD_PROTOCOL_NAME){
    //
    // }
    return result;
}

std::string DataServer::get(std::string &key, std::string &timestamp, std::string &curr_class){
    std::string result;
    if(curr_class == CAS_PROTOCOL_NAME){
        result = CAS.get(key, timestamp, cache, persistent, lock);
    }else if(curr_class == ABD_PROTOCOL_NAME){
        result = ABD.get(key, timestamp, cache, persistent, lock);
    }
    return result;
}
