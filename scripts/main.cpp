#include <iostream>
#include <string>
#include <vector>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sstream>
#include <cstring>
#include <fstream>

#define METADATA_IP     "0.0.0.0"
#define METADATA_PORT   "30000"


using namespace std;

enum Status{
    OK = 0,
    machine_init,
    machine_running,
    pipe_error,
    fork_error,
    child_read_error,
    chile_exec_error,
    created_machine_error,
    status_is_not_running,
    scp_exec_error
};

namespace Helpers{
	int execute(const char* command, const vector<string>& args, string& output){

        int pipedes[2];
        if(pipe(pipedes) == -1) {
            perror("pipe");
            return Status::pipe_error;
        }

        pid_t pid = fork();
        if (pid == -1){
            perror("fork");
            return Status::fork_error;
        }
        else if(pid == 0){ // Child process
            while ((dup2(pipedes[1], STDOUT_FILENO) == -1) && (errno == EINTR)) {} // we used while so it will happen even if an interrupt arises
            while ((dup2(pipedes[1], STDERR_FILENO) == -1) && (errno == EINTR)) {}
            close(pipedes[1]);
            close(pipedes[0]);
            switch(args.size()){
                case 1:
                    execl(command, args[0].c_str(), (char *) 0);
                    break;

                case 2:
                    execl(command, args[0].c_str(), args[1].c_str(), (char *) 0);
                    break;

                case 3:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), (char *) 0);
                    break;

                case 4:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), (char *) 0);
                    break;

                case 5:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), (char *) 0);
                    break;

                case 6:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), (char *) 0);
                    break;

                case 7:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), (char *) 0);
                    break;

                case 8:
                    execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), (char *) 0);
                    break;
            }
//            for(int i = 0; i < args.size(); i++){
//                delete args_c[i];
//            }
//            delete args_c;
            perror("execl");
            _exit(1);
        }
        else {
//            for(int i = 0; i < args.size(); i++){
//                delete args_c[i];
//            }
//            delete args_c;
            close(pipedes[1]);
            int ret_val;
            cout << "!!!!" << endl;
            wait(&ret_val);
            cout << "13333" << endl;

            ret_val = WEXITSTATUS(ret_val);

            char buffer[4096];
            while(true){
                ssize_t count = read(pipedes[0], buffer, sizeof(buffer));
                if (count == -1) {
                    if (errno == EINTR) {
                        continue;
                    }
                    else{
                        perror("read");
                        return Status::child_read_error;
                    }
                }
                else if(count == 0){
                    break;
                }
                else{
                    output.append(buffer, count);
                }
            }
            close(pipedes[0]);
            return ret_val;
        }

	}
}

class Machine{
private:
	string name;
	string type;
	string zone;
	int number_of_cpu;  // For later use
	int ram_size;	    // For later use

	string internal_ip;
	string external_ip;

	int status;



public:
    bool existed;

	Machine(const string& name, const string& type, const string& zone) : name(name), type(type), zone(zone){
        existed = false;
		
		//ToDo: obtain this information from the google cloud
		number_of_cpu = 0;
		ram_size = 0;

		status = Status::machine_init;


		// run the server
        vector<string> args = {"gcloud", "compute", "instances", "create", this->name, string("--machine-type=") + this->type, string("--zone=") + this->zone};
        string output;
        status = Helpers::execute("/home/shahrooz/google-cloud-sdk/bin/gcloud", args, output);

//        cout << status << "AAA " << endl;
		
        if(output != ""){
            if(output.find("ERROR") != string::npos){
                existed = true;
                status = Status::chile_exec_error;
                cerr << output << endl;
                vector<string> args = {"gcloud", "compute", "instances", "list"};
                output.clear();
                status = Helpers::execute("/home/shahrooz/google-cloud-sdk/bin/gcloud", args, output);
                cout << "SSS: " << output << endl;
                string line;
                stringstream s(output);
                while(!s.eof()){
                    getline(s, line);
                    size_t pos;
                    pos = line.find(' ');
                    if(line.substr(0, pos) == this->name){
                        stringstream s(line);
//                cout << "AAA: " << output.substr(pos + 1) << endl;
                        string a1, a2, a3, a4;
                        s >> skipws >> a1 >> a2 >> a3 >> this->internal_ip >> this->external_ip >> a4;
                        if(a4 == "RUNNING"){
                            cerr << "yeah: " << output << endl;
                            status = Status::machine_running;
                        }
                        else{
                            cerr << output << endl;
                            status = Status::created_machine_error;
                        }
                    }
                }

            }
            else{
                size_t pos;
                pos = output.find('\n');
                pos = output.find('\n', pos + 1);
//                cout << pos << " SSS" << endl;
                stringstream s(output.substr(pos + 1));
//                cout << "AAA: " << output.substr(pos + 1) << endl;
                string a1, a2, a3, a4;
                s >> skipws >> a1 >> a2 >> a3 >> this->internal_ip >> this->external_ip >> a4;
                if(a4 == "RUNNING"){
                    cerr << output << endl;
                    status = Status::machine_running;
                }
                else{
                    cerr << output << endl;
                    status = Status::created_machine_error;
                }
            }
        }
        else{
            status = Status::chile_exec_error;
            cerr << output << endl;
        }

	}


	~Machine(){}

	string get_name(){
	    return name;
	}
	string get_external_ip(){
	    return external_ip;
	}
	int get_status(){
        return status;
	}

	int copy_to(const string& path){

	    if(this->status != Status::machine_running) {
            return Status::status_is_not_running;
        }

        vector<string> args = {"scp", "-o", "StrictHostKeyChecking no", "-r", path, this->external_ip + ":"};
        string output;
        int status = Helpers::execute("/usr/bin/scp", args, output);
        cerr << output << endl;

        if(output != ""){
            if(output.find("ERROR") != string::npos){
                return scp_exec_error;
            }
            else{
                return Status::OK;
            }
        }
        else{
            cerr << "status is " << status << endl;
            return Status::scp_exec_error;
        }
	}

	int execute(const string& command){

        if(this->status != Status::machine_running) {
            return Status::status_is_not_running;
        }

        vector<string> args = {"ssh", "-o", "StrictHostKeyChecking no", "-t", this->external_ip, command};
        string output;
        int status = Helpers::execute("/usr/bin/ssh", args, output);

        if(status == Status::OK){
            cout << output << endl;
            return Status::OK;
        }
        else{
            if(output != ""){
                if(output.find("ERROR") != string::npos){
                    cerr << "error in execution " << command << endl;
                    cerr << output << endl;
                    return scp_exec_error;
                }
            }
            else{
                cerr << "WARN: ret value in execution " << command << " is not zero and output is empty." << endl;
                return scp_exec_error;
            }
        }


	}
};

static vector<Machine> machines;
static string setup_config;

int write_setup_config(){
    setup_config = "{\n";
    for(int i = 0; i < machines.size(); i++){
        setup_config += "    \"";
        setup_config += to_string(i + 1);
        setup_config += "\" : {\n";

        setup_config += "        \"metadata_server\" : {\n";
        setup_config += "            \"host\" : \"";
        setup_config += METADATA_IP;
        setup_config += "\",\n";
        setup_config += "            \"port\" : \"";
        setup_config += METADATA_PORT;
        setup_config += "\"\n";
        setup_config += "        },\n";
        setup_config += "        \"servers\" : {\n";
        setup_config += "            \"1\" : {\n";
        setup_config += "                \"host\" : \"";
        setup_config += m1.get_external_ip();
        setup_config += "\",\n";
        setup_config += "                \"port\" : \"10000\"\n";
        setup_config += "            }\n";
        setup_config += "        }\n";

        setup_config += "    }";
        if(i != machines.size() - 1){
            setup_config += ",\n";
        }
        else{
            setup_config += "\n";
        }
    }

    setup_config += "}";

    ofstream out("setup_config.json", ios::out);
    out << setup_config;
    out.close();

    return 0;
}


int main(){

    vector<string> zones = {"us-central1-a", "us-east4-a", "australia-southeast1-c", "southamerica-east1-a", "asia-south1-b",
                                "northamerica-northeast1-a", "europe-north1-a", "asia-east2-c", "europe-west6-b", "asia-northeast2-b",
                                "us-west3-a", "asia-southeast2-a"};

	// Reading machine-types and zones
//	machines.push_back("s1", "e2-micro", "us-central1-a")

    Machine m1("s4", "e2-micro", "us-central1-a");
    machines.push_back(m1);

    cout << "the ip of the created machine is " << m1.get_external_ip() << endl;

    write_setup_config();

    sleep(20);
    if(!m1.existed) {
        system("./copy.sh"); // Full copy

        // Project compile requirement
        m1.execute("sudo apt-get install -y build-essential autoconf automake libtool zlib1g-dev git protobuf-compiler pkg-config");
        m1.execute("git clone https://github.com/openstack/liberasurecode.git");
        m1.execute("cd liberasurecode/; ./autogen.sh; ./configure --prefix=/usr; make -j3");
        m1.execute("cd liberasurecode/; sudo make install");
    }
    else {
        system("./copy2.sh"); // partial copy
    }

    m1.copy_to("/home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/project.tar.gz");

    m1.execute("tar -xzf project.tar.gz; cd project/; make proto; make cleanall; make -j3"); // compile the project
    m1.execute("cd project/; ./Server 10000 db1.temp > Server_output.txt &"); // execution

	return 0;
}
