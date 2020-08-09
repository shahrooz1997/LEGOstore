/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   ABD_Server.h
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:35 PM
 */

#ifndef ABD_Server_H
#define ABD_Server_H

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
using std::string;

class ABD_Server {
public:
    ABD_Server();
    ABD_Server(const ABD_Server& orig);
    virtual ~ABD_Server();

    string get_timestamp(string &key, Cache &cache, Persistent &persistent, std::mutex &lock_t);
	string put(string &key, string &value, string &timestamp, Cache &cache, Persistent &persistent, std::mutex &lock_t);
    string get(string &key, string &timestamp, Cache &cache, Persistent &persistent, std::mutex &lock_t);
private:

};

#endif /* ABD_Server_H */
