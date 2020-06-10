#ifndef DATA_SERVER_H
#define DATA_SERVER_H

#include "Cache.h"
#include "Persistent.h"
#include "CAS_Server.h"

typedef std::vector<std::string> strVec;

class DataServer{

public:
	//TODO:: confirm the backlog value, set to 2048 in py
	// TODO:: removed the null value of socket, directly intializing class variable

	DataServer(std::string directory, int sock)
		: sockfd(sock),cache(500000000), persistent(directory){}

	int getSocketDesc();
	strVec get_timestamp(std::string &key, std::string &curr_class);
	strVec put(std::string &key, std::string &value, std::string &timestamp, std::string &curr_class);
	strVec put_fin(std::string &key, std::string &timestamp, std::string &curr_class);
	strVec get(std::string &key, std::string &timestamp, std::string &curr_class);

private:

	int sockfd;
	std::mutex lock;
	Cache cache;
	Persistent persistent;
	CAS_Server CAS;
};

#endif
