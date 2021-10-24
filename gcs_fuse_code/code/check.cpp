#include<iostream>
#include<fstream>
using namespace std;
int main()
{

	for(int i=0;i<20;i++)
	{
		ofstream out(string ("../gcs_mount/test.txt") + to_string(i));
		out << "Hello"<<endl;
	}
		// out.close();
	return 0;
}
