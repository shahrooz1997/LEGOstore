#include <pthread.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <stdlib.h>
#include <string>
#include <sys/random.h>
#include <errno.h>
#include <stdbool.h>

typedef struct thread_args 
{
	rocksdb::DB *db;
	std::string key;
	bool critical_debug;
}Thread_args;

