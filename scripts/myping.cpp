#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

using namespace std;

static std::vector<string> ips;
static std::vector<double> latencies;

int execute(const char* command, const vector<string>& args, string& output){

    cout << "Executing \"" << command << " ";
    for(int i = 1; i < args.size(); i++){
        cout << args[i];
        if(i != args.size() - 1)
            cout << " ";
    }
    cout << "\"...\n";


    int pipedes[2];
    if(pipe(pipedes) == -1) {
        perror("pipe");
        return -1;
    }

    // int pipedes2[2];
    // if(pipe(pipedes2) == -1) {
    //     perror("pipe");
    //     return Status::pipe_error;
    // }


    pid_t pid = fork();
    if (pid == -1){
        perror("fork");
        return -2;
    }
    else if(pid == 0){ // Child process
        while ((dup2(pipedes[1], STDOUT_FILENO) == -1) && (errno == EINTR)) {} // we used while so it will happen even if an interrupt arises
        while ((dup2(pipedes[1], STDERR_FILENO) == -1) && (errno == EINTR)) {}
        close(pipedes[1]);
        close(pipedes[0]);
        // while ((dup2(pipedes2[2], STDIN_FILENO) == -1) && (errno == EINTR)) {}
        // close(pipedes2[1]);
        // close(pipedes2[0]);
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

            case 9:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), (char *) 0);
                break;

            case 10:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), (char *) 0);
                break;

            case 11:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), (char *) 0);
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
        // close(pipedes2[2]);
        int ret_val;
        wait(&ret_val);

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
                    return -3;
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

template <class Container>
void splitbyline(const std::string& str, Container& cont,
              char delim = '\n')
{
    std::size_t current, previous = 0;
    current = str.find(delim);
    while (current != std::string::npos) {
        cont.push_back(str.substr(previous, current - previous));
        previous = current + 1;
        current = str.find(delim, previous);
    }
    cont.push_back(str.substr(previous, current - previous));
}

double average(const string& in){
	std::vector<string> lines;
	splitbyline(in, lines);

	double total = 0;
	uint counter = 0;
	for(int i = 1; i <= 10; i++){
		stringstream s(lines[i]);
		// cout << "| " << lines[i] << " |" << endl;
	    string a0, a1, a2, a3, a4, a5, a6;
	    s >> skipws >> a0 >> a1 >> a2 >> a3 >> a4 >> a5 >> a6;
	    // cout << "| " << a6 << " |" << endl;
	    string time = a6.substr(a6.find("=") + 1);
	    // cout << "| " << time << " |" << endl;

	    total += stod(time);
	    counter++;
	}

	return total / counter;
}

int main(int argc, char* argv[]){

	if(argc != 11){
		cout << "Usage: " << argv[0] << " server_number ip1 ip2 ... ip9" << endl;
		return -1;
	}

	for(uint i = 2; i <= 10; i++){
		ips.push_back(argv[i]);
	}

	for(uint i = 0; i < 9; i++){
		vector<string> args;
		string output;
		args.push_back("ping");
		args.push_back(ips[i]);
		args.push_back("-c");
		args.push_back("10");
		execute("/bin/ping", args, output);
		latencies.push_back(average(output));
	}


	ofstream out(string("latencies_from_server_") + argv[1] + ".txt", ios::out);
	for (uint i = 0; i < latencies.size() - 1; ++i){
		out << latencies[i] << " ";
	}
	out << latencies.back() << endl;
	out.close();

	return 0;
}
