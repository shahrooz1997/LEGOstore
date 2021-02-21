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


class CAS_Server{
public:
    CAS_Server(const std::shared_ptr<Cache>& cache_p, const std::shared_ptr<Persistent>& persistent_p,
               const std::shared_ptr<std::vector<std::unique_ptr<std::mutex>>>& mu_p_vec_p);
    CAS_Server(const CAS_Server& orig) = delete;
    virtual ~CAS_Server();

    std::string get_timestamp(const std::string& key, uint32_t conf_id);
    std::string put(const std::string& key, uint32_t conf_id, const std::string& value, const std::string& timestamp);
    std::string put_fin(const std::string& key, uint32_t conf_id, const std::string& timestamp);
    std::string get(const std::string& key, uint32_t conf_id, const std::string& timestamp);

    strVec init_key(const std::string& key, const uint32_t conf_id);

private:
    std::shared_ptr<Cache> cache_p;
    std::shared_ptr<Persistent> persistent_p;
    std::shared_ptr<std::vector<std::unique_ptr<std::mutex>>> mu_p_vec_p;

    strVec get_data(const std::string& key);
    int put_data(const std::string& key, const strVec& value);
};

#endif /* CAS_Server_H */
