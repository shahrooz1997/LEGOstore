# Compile the code

go build .

# Run the binary

./csChecker <path_to_logs_file> <create_visualization>

# Example: Reads all files in the directory log_files/ and creates a visual graph of the history

./csChecker "./log_files" 1

Note: Provide the initial value of your keys as part of the Init function in Model struct.  
The model has been specified in the file main.go  


#### LOG FORMAT #####

ClientID, get, Key, Value_read, call_time, return_time  	
ClientID, put, Key, Value_written, call_time, return_time  


ClientID -> int32  
Key, Value -> string  
call_time, return_time -> int64  
