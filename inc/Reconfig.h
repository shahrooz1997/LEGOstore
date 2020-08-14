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

#include "Util.h"
#include <stdint.h>
#include <string>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "Timestamp.h"
#include <erasurecode.h>
#include "Data_Transfer.h"

class Reconfig {
public:

    int start_reconfig(Properties *prop, GroupConfig &old_config, GroupConfig &new_config, std::string key, int old_desc, int new_desc);

    int get_metadata_info(std::string &key, GroupConfig **old_config);
    int update_metadata_info(std::string &key, GroupConfig *new_config);

private:

    static int send_reconfig_query(Properties *prop, GroupConfig &old_config, std::string &key, Timestamp **ret_ts, std::string &ret_v);
    static int send_reconfig_finalize(Properties *prop, GroupConfig &old_config, std::string &key, Timestamp *ret_ts, std::string &ret_v, int desc);
    static int send_reconfig_write(Properties *prop, GroupConfig &new_config, std::string &key, Timestamp *ret_ts, std::string &ret_v, int desc);
    static int send_reconfig_finish(Properties *prop, GroupConfig &old_config, GroupConfig &new_config, std::string &key, Timestamp *ret_ts);

    std::unordered_map<std::string, GroupConfig*> key_metadata;
    std::mutex lock_t;
};

#endif /* RECONFIG_H */
