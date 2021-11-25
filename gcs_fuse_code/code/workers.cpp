/*

For information about this program check README.md in the same directory as this file.

*/
#include "workers.h"
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <string>


static void * writer(void *thread_args)
{
	Thread_args *args = (Thread_args *) thread_args;
	
	long int rounds = random();
	
	// Don't want more than 50 rounds
	rounds %= 50;

	printf("Rounds chosen for Thread %lu is %lu\n", pthread_self(), rounds);

	char random_buffer[100];
	std::string check_buffer;
	
	for(int round_num = 0; round_num < rounds; round_num++)
	{
		printf("[Thread %lu] round number = %d\n", pthread_self(), round_num+1);
		
		getrandom(random_buffer,100,0);
		// Delete this dynamically allocated Status object after printing in main()
		rocksdb::Status *status = new rocksdb::Status;
		rocksdb::WriteOptions write_options;
		write_options.sync = true;
		*status = args->db->Put(write_options, args->key, random_buffer);
		if(!status->ok())
		{
			return status;
		}
		
		// This is to check if above Put's value is obtained in Get
		// This is not critical in fact expected
		args->db->Get(rocksdb::ReadOptions(), args->key, &check_buffer);
		if(!status->ok())
		{
			return status;
		}

		// The below check is not critical because there could be a case
		// when current thread is context switched after write and some other thread
		// did a Put operation just after that.
		if(args->critical_debug && check_buffer.compare(0,100,random_buffer))
		{
			printf("[Thread %lu] Immediate write-read compare failed\n", pthread_self());
		}
		delete status;
	}

	return NULL;
	
}

int main(int argc, char *argv[])
{
	if((argc < 5) || (strcmp(argv[3],"--critical-debug") && (strcmp(argv[4],"true") || strcmp(argv[4],"false"))))
	{
		printf("\n\n\
				\nMissing number of threads:\
				\ntry: ./writer <total_threads> <key to use> --critical-debug [false|true]\
				\nExample: ./writer 10 34 --critical-debug false\
				\n\n");
		return 1;
	}
	
	Thread_args thread_args;
	
	// Setup rocksdb
	rocksdb::DB* db;
	rocksdb::Options options;
	options.create_if_missing = true;
	
	// Directory where you want to mount rocksdb as your database 
	std::string rocksdb_mount_path = "../gcs_mount/";

	rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_mount_path, &db);
	printf("Open db status: code = %d subcode = %d\n", status.code(), status.subcode());
	assert(status.ok());
	
	std::vector<std::pair <pthread_t,rocksdb::Status>> pthreads;
	
	// Handling args
	int total_threads = std::stoi(argv[1]);
	thread_args.db = db;
	//Use key passed as args to the program as key for rocksdb
	thread_args.key = argv[2];
	thread_args.critical_debug = (strcmp(argv[4],"true") ? false : true);

	pthread_t thread_id;
	
	for(int thread_num = 0; thread_num < total_threads; thread_num++)
	{
		if(pthread_create(&thread_id, NULL, &writer, &thread_args) == 0)
		{
			printf("\nThread create succesfully with id: %lu\n", thread_id);
			pthreads.push_back(std::make_pair(thread_id,rocksdb::Status()));
		}

	}	
	
	// loop to join all threads and store their return status.
	for(auto thread_info : pthreads)
	{
		pthread_t thread_id = thread_info.first;
		void* status;

		int join_status = pthread_join(thread_id, &status);
		
		if(join_status)
		{
			printf("Thread join failed on Thread %lu with error number %d\n", thread_id, errno);
		}
			
		// status will be NULL if every operation was successful	
		if(status)
		{
			// Store status in thread_info
			thread_info.second = *(rocksdb::Status *) status;

			// status was dynamically allocated in function writers
			// This trick is need because delete cannot directly act on
			// void * type.
			rocksdb::Status *temp_status = (rocksdb::Status *) status; 
			delete temp_status;
		}	
	}
	
	printf("\n\n\n\
			\nThread level status code report\
			\n\n");
	
	// Print return status of each thread to see if there were any issue during operation.s
	for(auto thread_info : pthreads)
	{
		printf("[Thread %lu] Status code = %d subcode = %d \n", \
				thread_info.first, //(pthread_t) 
			       	thread_info.second.code(), //(rocksdb::Status::Code) 
				thread_info.second.subcode() //(rocksdb::Status::SubCode)
		      );
	}
	
	delete db;
	return 0;
}
