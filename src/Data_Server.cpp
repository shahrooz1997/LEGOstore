#include "Data_Server.h"

int DataServer::getSocketDesc(){
    return sockfd;
}

valueVec DataServer::get_timestamp(std::string &key, std::string &curr_class){

    if(curr_class == "CAS"){
        return CAS.get_timestamp(key,cache, persistent, lock);
    }else if(curr_class == "ABD"){

    }
}

valueVec DataServer::put(std::string &key, std::string &value, std::string &timestamp, std::string &curr_class){
    if(curr_class == "CAS"){
        return CAS.put(key,value, timestamp, cache, persistent, lock);
    }else if(curr_class == "ABD"){

    }
}

valueVec DataServer::put_fin(std::string &key, std::string &timestamp, std::string &curr_class){

    if(curr_class == "CAS"){
        return CAS.put_fin(key, timestamp, cache, persistent, lock);
    }else if(curr_class == "ABD"){

    }
}

valueVec DataServer::get(std::string &key, std::string &timestamp, std::string &curr_class){

    if(curr_class == "CAS"){
        return CAS.get(key, timestamp, cache, persistent, lock);
    }else if(curr_class == "ABD"){

    }
}
