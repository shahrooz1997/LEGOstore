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


#ifdef DEVELOPMENT
bool DEBUG_CAS_Client = true;
bool DEBUG_ABD_Client = true;
bool DEBUG_CAS_Server = true;
bool DEBUG_ABD_Server = true;
#else
bool DEBUG_CAS_Client = false;
bool DEBUG_ABD_Client = false;
bool DEBUG_CAS_Server = false;
bool DEBUG_ABD_Server = false;
#endif


JSON_Reader::JSON_Reader() {
}

JSON_Reader::~JSON_Reader() {
}

