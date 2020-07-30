#ifndef _CACHE_H_
#define _CACHE_H_

#include "lrucache.h"
#include <string>
#include <vector>


class Cache{

public:
	Cache(size_t size);
	
	size_t get_size(const std::vector<std::string> &value);
	void put(const std::string &key, const std::vector<std::string> &value);

	const std::vector<std::string>* get(const std::string &key);

	size_t get_current_size();
	bool exists(const std::string &key);
	void modify_flag(const std::string &key, int label);
private:
	size_t max_size;
	size_t curr_size;
	LRU::lru_cache<std::string, std::vector<std::string> > cache_obj;

};

#endif 
