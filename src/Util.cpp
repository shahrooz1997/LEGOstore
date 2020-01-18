/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Util.cpp
 * Author: shahrooz
 * 
 * Created on January 18, 2020, 5:18 PM
 */

#include "Util.h"

bool DEBUG_CAS_Client = false;
bool DEBUG_ABD_Client = false;
bool DEBUG_CAS_Server = false;
bool DEBUG_ABD_Server = false;


// ToDo: ask Nader if we should have some default values
// Default values is defined due to the default config file.
uint32_t local_datacenter_id = 1;
uint32_t retry_attempts = 1;
uint32_t metadata_server_timeout = 120;
uint32_t timeout_per_request = 120;
uint32_t duration = 20;

JSON_Reader::JSON_Reader() {
}

JSON_Reader::~JSON_Reader() {
}

