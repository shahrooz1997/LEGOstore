/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   CAS_Server.cpp
 * Author: shahrooz
 * 
 * Created on January 4, 2020, 11:35 PM
 */

#include "CAS_Server.h"

using std::string;

CAS_Server::CAS_Server() {
}


CAS_Server::~CAS_Server() {
}

//TODO:: fix the lock
strVec CAS_Server::get_timestamp(string &key, Cache &cache, Persistent &persistent, std::mutex &lock_t){

	//TODO:: check the different read and write lock
	std::lock_guard<std::mutex> lock(lock_t);
	const std::vector<std::string> *ptr = cache.get(key);
	string data; // = (*cache.get(key))[0];

	if(ptr == nullptr){
		data = persistent.get(key)[0];
	}else{
		data = (*ptr)[0];
	}	
	
	if(data.empty()){
		return {"Failed", "None"};
				
	}else{
		return {"OK", data};
	}	
}

strVec CAS_Server::put(string &key, string &value, string &timestamp, Cache &cache, Persistent &persistent, std::mutex &lock_t){

	std::lock_guard<std::mutex> lock(lock_t);
	insert_data(key, value, timestamp, false, cache, persistent);
	return {"OK"};
}

strVec CAS_Server::put_fin(string &key, string &timestamp, Cache &cache, Persistent &persistent, std::mutex &lock_t){

	std::lock_guard<std::mutex> lock(lock_t);
	insert_data(key, string(), timestamp, true, cache, persistent);
	return {"OK"};
}


//Add client ID
void CAS_Server::insert_data(string &key,const string &val, string &timestamp, bool label, Cache &cache, Persistent &persistent){
	
	//TODO:: Can we avoid this??
	// Why is this there
	//(cache.exists(key+timestamp)){
	//	return;
	//}
	
	//TODO:: ALready in FIN, so no way that we need to store naything , cos no way values 
	// can come now in a msg right???
	//TODO:: should i add it to cache on write, from persistent
	//If not found, that means either tuple not present
	// OR tuple is in pre stage
	bool fnd = cache.exists(key);
	if(!fnd){
		fnd = persistent.exists(key);	
	}
	if(fnd){
		return;
	}

	int _label = label? 1:0;
	std::vector<string> value{val, std::to_string(_label)};

	// Try to find the tuple
	fnd = cache.exists(key+timestamp);
	if(!fnd){
		fnd = persistent.exists(key+timestamp);
		
		// If Tag not seen before, add the tuple 
		if(!fnd){
			//TODO:: optimize and create threads here
			// For thread, add label check here and then return
			// Also add fin tag to both
			cache.put(key+timestamp, value);
			persistent.put(key+timestamp, value);
			if(label){
				cache.put(key, {timestamp});
				persistent.put(key, {timestamp});
			}
			return;
		}	
	//TODO::should I add to cache
	// Modify_flag in cache can't be used, cos may not even be in cache
	}	
	
	//TODO:: If found, then modify values
	if(label){
		// TODO:: check if not found, does LAbel update make sense??	
		cache.put(key, {timestamp});
		persistent.put(key, {timestamp});
		// Use modify function, cos don't want to overwrite value with NULL
		// Take care of case where data not in Cache	
		cache.modify_flag(key+timestamp,_label);
		persistent.modify_flag(key+timestamp, _label);
	}

}

 
