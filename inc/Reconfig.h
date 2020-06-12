/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Reconfig.h
 * Author: shahrooz
 *
 * Created on March 23, 2020, 6:34 PM
 */

#ifndef RECONFIG_H
#define RECONFIG_H

#include <thread>
#include <mutex>
#include <condition_variable>
#include "Util.h"
#include "Timestamp.h"


//struct Config{
//    
//    Config(Timestamp &timestamp, Group &group); // You may set the client id in timestamp to -1
//    
//    Timestamp *timestamp;// ToDo: do we need this?
//    Group *group;
//    
//    virtual ~Config();
//};

class Reconfig {
public:
    
    static int reconfig(GroupConfig &old_config, GroupConfig &new_config);
    static Reconfig* get_instance();
    
    Reconfig(const Reconfig& orig) = delete;
    virtual ~Reconfig();
    
    uint32_t get_operation_id();

private:
    Reconfig(); // Singleton
    static Reconfig *instance;
    uint32_t operation_id;
    uint32_t id;
    FILE* log_file;
    
    int send_reconfig_query(GroupConfig &old_config, Timestamp* ret_ts, string *ret_v);
    int send_reconfig_finalize(GroupConfig &old_config);
};

#endif /* RECONFIG_H */

