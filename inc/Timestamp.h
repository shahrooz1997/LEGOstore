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

#include <string>
#include <stdint.h>
#include <vector>
#include <assert.h>

class Timestamp {
public:
    Timestamp(uint32_t client_id, uint32_t time = 0);
    Timestamp(const Timestamp &t); // Copy constructor
    std::string get_string();
    Timestamp increase_timestamp(const uint32_t client_id);
    virtual ~Timestamp();
    
    static Timestamp& max_timestamp(std::vector<Timestamp*>& v);

private:
	uint32_t time;
	uint32_t client_id;
};

#endif /* Timestamp_H */

