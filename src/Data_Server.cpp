#include "Data_Server.h"

int DataServer::getSocketDesc(){
    return sockfd;
}

std::string DataServer::get_timestamp(std::string &key, std::string &curr_class){

    std::string result;
    if(curr_class == "CAS"){
        result = CAS.get_timestamp(key,cache, persistent, lock);
    }else if(curr_class == "ABD"){

    }

    return result;
}

std::string DataServer::reconfig_query(std::string &key, std::string &curr_class){

    std::string result;
    if(curr_class == "CAS"){
        result = CAS.get_timestamp(key,cache, persistent, lock);
    }else if(curr_class == "ABD"){
        // Directly do a get here for ABD i think

    }

    return result;
}

std::string DataServer::reconfig_finalize(std::string &key, std::string &timestamp, std::string &curr_class){
    std::string result;
    if(curr_class == "CAS"){
        result = CAS.get(key, timestamp, cache, persistent, lock);
    }else{
        //Error scenario;
    }
    return result;
}

std::string DataServer::write_config(std::string &key, std::string &value, std::string &timestamp, std::string &curr_class){

    std::string result;
    if(curr_class == "CAS"){
        result = CAS.put(key,value, timestamp, cache, persistent, lock);
        result = CAS.put_fin(key, timestamp, cache, persistent, lock);
    }else if(curr_class == "ABD"){

    }

    return result;
}

std::string DataServer::put(std::string &key, std::string &value, std::string &timestamp, std::string &curr_class){
    std::string result;
    if(curr_class == "CAS"){
        result = CAS.put(key,value, timestamp, cache, persistent, lock);
    }else if(curr_class == "ABD"){

    }
    return result;
}

std::string DataServer::put_fin(std::string &key, std::string &timestamp, std::string &curr_class){
    std::string result;
    if(curr_class == "CAS"){
        result = CAS.put_fin(key, timestamp, cache, persistent, lock);
    }else if(curr_class == "ABD"){

    }
    return result;
}

std::string DataServer::get(std::string &key, std::string &timestamp, std::string &curr_class){
    std::string result;
    if(curr_class == "CAS"){
        result = CAS.get(key, timestamp, cache, persistent, lock);
    }else if(curr_class == "ABD"){

    }
    return result;
}
