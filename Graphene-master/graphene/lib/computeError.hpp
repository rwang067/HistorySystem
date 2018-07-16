#include <cmath>
#include <iostream>
#include <fstream>
typedef std::pair<int,float> v_t;
template <typename T>
v_t* regularize(std::pair<int,T> *v, int n){
    T sum=0;
    v_t* ans = new v_t[n];
    for (int i=0; i<n; i++){
        sum+=v[i].second;
    }
    for (int i=0; i<n; i++){
        ans[i].first = v[i].first;
        ans[i].second = v[i].second*1.0/sum;
    }
    return ans;
}

int dumpIntoFile(v_t* v, int ntop, char* outputFilename){
    std::ofstream of(outputFilename);
    for (int i=0;i < ntop; i++){
        of << v[i].first << '\t' << v[i].second<<std::endl;
    }
    of.close();
    return 0;
}

v_t* readFromFile(int ntop, char* inputFilename){
    std::ifstream inf(inputFilename);
    v_t* v=new v_t[ntop];
    for (int i=0; i<ntop; i++){
        inf >> v[i].first >> v[i].second;
    }
    inf.close();
    return v;
}

float computeError(v_t* s, v_t* t, int ntop){  //assume ntop < 100, O(ntop^2)
    float err=0;
    int cnt = 0;
    for (int i =0; i<ntop; i++){
        for (int j =0; j<ntop; j++){
            if (t[i].first == s[j].first){
                err += fabs(t[i].second-s[j].second)/s[j].second;
                cnt++;
                break;
            }
        }
    }
    err /= ntop;
    return err;
}

/*
    std::pair<int,float>* zq_top = new std::pair<int,float>[ntop];
    for (int i=0;i<ntop;i++){
        zq_top[i].first = top[i].vertex;
        zq_top[i].second = top[i].value;
    }

    std::pair<int,float>* reg_top = regularize(zq_top, 100);
    delete[] zq_top;
}
*/
