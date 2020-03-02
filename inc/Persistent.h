#ifndef _PERSISTENT_H_
#define _PERSISTENT_H_

#include "rocksdb/db.h"
#include <cassert>
#include <iostream>

class Persistent{

public:
	Persistent(const std::string &directory);
	
	const std::vector<std::string> get(const std::string &key);

	void put(const std::string &key, std::vector<std::string> value);

	std::string& encode(std::vector<std::string> &value);

	std::vector<std::string> decode(const std::string &value);

	void modify_flag(const std::string &key, int label);

	bool exists(const std::string &key);	

private:
	rocksdb::DB *db;	
	
};


#endif
