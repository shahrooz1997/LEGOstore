/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   Timestamp.h
 * Author: shahrooz
 *
 * Created on January 4, 2020, 11:35 PM
 */

#ifndef Timestamp_H
#define Timestamp_H

#include "Util.h"
#include <stdint.h>
#include <assert.h>

class Timestamp {
public:
    Timestamp(uint32_t client_id, uint32_t time = 0);
    Timestamp(const Timestamp &t); // Copy constructor
    Timestamp(std::string &str);
    std::string get_string();
    Timestamp increase_timestamp(const uint32_t client_id);
    virtual ~Timestamp();

    static Timestamp& max_timestamp(std::vector<Timestamp*>& v);
    static Timestamp& max_timestamp2(std::vector<Timestamp>& v);
    static uint32_t max_timestamp3(std::vector<Timestamp>& v);

    static bool compare_timestamp(std::string left, std::string right);
    friend bool operator > (Timestamp &lhs, Timestamp &rhs);

private: 
	uint32_t time;
	uint32_t client_id;
};


#endif /* Timestamp_H */
