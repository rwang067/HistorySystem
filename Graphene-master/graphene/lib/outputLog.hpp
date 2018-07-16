#include "computeError.hpp"
#include <string>
#include <fstream>
bool res_compare(std::pair<int,float> a, std::pair<int,float> b){
	return a.second > b.second;
}

int printLog(int level, int vert_count, sa_t* sa_curr, const char* beg_dir, bool output_sorted=true){
	std::cout<<"printLog: start"<<std::endl;
	char level_str[3];
	sprintf(level_str, "%d", level);
	std::string outputFileDir = beg_dir;
	std::string outputFileName;
	std::ofstream outputFile;

	if (output_sorted){
		//std::pair<int, float> res[vert_count];
		std::pair<int, float> *res = new std::pair<int,float>[vert_count];
		for (int i = 0 ; i < vert_count; i++){
			res[i].first = i;
			res[i].second = sa_curr[i];
		}
		sort(res,res+vert_count,res_compare);
		int ntop=100;
		std::pair<int,float> *tt = regularize(res,ntop);
		std::pair<int,float> *ss = readFromFile(ntop,"std.vout");
		float err = computeError(ss,tt,ntop);
		std::cout<<"zq:\t" << "err=" << err << std::endl;
		outputFileName = outputFileDir + "/sa_curr_sorted" + level_str + ".txt";
		std::cout<<outputFileName<<std::endl;
		outputFile.open(outputFileName.c_str());
		outputFile << "vert_id" << "\t" << "sa_curr_value" << "\n";
		for (int i = 0 ; i < vert_count; i++){
			outputFile << res[i].first << "\t" << res[i].second << "\n";
		}
		delete []res;
		outputFile.close();
	}
	
	outputFileName = outputFileDir + "/sa_curr" + level_str + ".txt";
	outputFile.open(outputFileName.c_str());
	outputFile << "vert_id" << "\t" << "sa_curr_value" << "\n";
	for (int i = 0 ; i < vert_count; i++){
		outputFile << i << "\t" << sa_curr[i] << "\n";
	}
	outputFile.close();

	std::cout<<"printLog: end"<<std::endl;
}
