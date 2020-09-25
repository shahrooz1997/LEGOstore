/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   CAS_Server.h
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:35 PM
 */

#ifndef CAS_Server_H
#define CAS_Server_H

#include <thread>
#include <vector>
#include <string>
#include <cstdlib>
#include "Cache.h"
#include "Persistent.h"
#include <mutex>
#include "Util.h"
#include "Timestamp.h"
#include "Data_Transfer.h"
#include <map>


using std::string;

class CAS_Server{
public:
    CAS_Server(std::map <std::string, std::vector<Request>>* recon_keys, std::string* metadata_server_ip,
            std::string* metadata_server_port);
    
    CAS_Server(const CAS_Server& orig) = delete;
    
    virtual ~CAS_Server();
    
    string get_timestamp(const string& key, uint32_t conf_id, const Request& req, Cache& cache, Persistent& persistent,
            std::mutex& lock_t);
    
    string put(string& key, uint32_t conf_id, string& value, string& timestamp, const Request& req, Cache& cache,
            Persistent& persistent, std::mutex& lock_t);
    
    string
    put_fin(string& key, uint32_t conf_id, string& timestamp, const Request& req, Cache& cache, Persistent& persistent,
            std::mutex& lock_t);
    
    string
    get(string& key, uint32_t conf_id, string& timestamp, const Request& req, Cache& cache, Persistent& persistent,
            std::mutex& lock_t);

private:
    Reconfig_key_info* create_rki(const std::string& key, const uint32_t conf_id);
    int init_key(const string& key, const uint32_t conf_id, Cache& cache, Persistent& persistent);
    
    int reconfig_info(const string& key, uint32_t conf_id, string& timestamp, const Request& req, string& msg,
            string& recon_timestamp, Cache& cache, Persistent& persistent);
    
    std::map <std::string, std::vector<Request>>* recon_keys;
    std::string* metadata_server_ip;
    std::string* metadata_server_port;
};

#endif /* CAS_Server_H */
