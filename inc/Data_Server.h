#ifndef DATA_SERVER_H
#define DATA_SERVER_H

#include "Cache.h"
#include "Persistent.h"
#include "CAS_Server.h"
#include "ABD_Server.h"
#include <unordered_map>
#include <condition_variable>
#include "Timestamp.h"

typedef std::vector <std::string> strVec;

class DataServer{

public:
    // TODO:: removed the null value of socket, directly intializing class variable
    
    DataServer(std::string directory, int sock, std::string metadata_server_ip = "127.0.0.1", std::string metadata_server_port = "30000");
    
    int getSocketDesc();
    
    std::string get_timestamp(std::string& key, std::string& curr_class, uint32_t conf_id, const Request& req);
    
    std::string
    put(std::string& key, std::string& value, std::string& timestamp, std::string& curr_class, uint32_t conf_id,
            const Request& req);
    
    std::string
    put_fin(std::string& key, std::string& timestamp, std::string& curr_class, uint32_t conf_id, const Request& req);
    
    std::string
    get(std::string& key, std::string& timestamp, std::string& curr_class, uint32_t conf_id, const Request& req);
    
    std::string reconfig_query(std::string& key, std::string& curr_class, uint32_t conf_id);
    
    std::string reconfig_finalize(std::string& key, std::string& timestamp, std::string& curr_class, uint32_t conf_id);
    
    std::string write_config(std::string& key, std::string& value, std::string& timestamp, std::string& curr_class,
            uint32_t conf_id);

private:
    
    int sockfd;
    std::mutex lock;
    Cache cache;
    Persistent persistent;
    CAS_Server CAS;
    ABD_Server ABD;
    
    std::string metadata_server_ip;
    std::string metadata_server_port;
    
    std::map <std::string, std::vector<Request>> recon_keys; // To maintain blocked requests
};

#endif
