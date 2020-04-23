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


struct Config{
    
    Config(Timestamp &timestamp, Group &group); // You may set the client id in timestamp to -1
    
    Timestamp *timestamp;// ToDo: do we need this?
    Group *group;
    
    virtual ~Config();
};

class Reconfig {
public:
    
    static int reconfig(Config &old_config, Config &new_config);
    
    Reconfig(const Reconfig& orig) = delete;
    virtual ~Reconfig();

private:
    Reconfig(); // Singleton
    static Reconfig *instance;
    
    int send_reconfig_query(Config &old_config);
    int send_reconfig_finalize(Config &old_config, Config &new_config);
};

#endif /* RECONFIG_H */

