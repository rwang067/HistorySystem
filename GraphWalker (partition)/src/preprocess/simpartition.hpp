#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>

#include "logger/logger.hpp"
#include "api/filename.hpp"
#include "api/io.hpp"

class SimPartition
{
private:
    std::string filename;
    int invlid;
    int invlnum;
    int membudget_mb;
    int invlsize;
    int cursize;
    vid_t stv, env;
    std::vector<std::pair<vid_t, vid_t>> invls;
public:
    SimPartition(std::string inputfile){
    	filename = inputfile;
    };
    ~SimPartition(){};

    void computeInvlSize(){
        membudget_mb = 800;
        invlsize = 10*1024 * 1024 ;
    }

    void bwritezero( char * buf, char * &bufptr, int count ){
        while( count-- ){
            *((int*)bufptr) = 0;
            bufptr += sizeof(int);
        }
    }

    void bwrite( char * buf, char * &bufptr, int count, std::vector<vid_t> outv ){
        if( cursize + count + 1 > invlsize ){
            std::string invlname = intervalname(filename,invlid);
            writefile(invlname, buf, bufptr);
            std::pair<vid_t, vid_t> invl(stv, env-1);
            invls.push_back(invl);
            std::cout << invlid << " " << stv << " " << env-1 << std::endl;
            stv = env;
            invlid++;
            cursize = 0;
        }
        *((int*)bufptr) = count;
        // if( env < 10 ) std::cout << *((int*)bufptr) << " : ";
        bufptr += sizeof(int);
        for( int i = 0; i < count; i++ ){
            *((vid_t*)bufptr) = outv[i];
             // if( env < 10 )  std::cout << *((int*)bufptr) << " ";
            bufptr += sizeof(vid_t);
        }
        // if( env < 10 )  std::cout << std::endl;
        cursize += count + 2;
        env++;
    }

    std::vector<std::pair<vid_t,vid_t>> partition(){
        computeInvlSize();
        FILE * inf = fopen(filename.c_str(), "r");
        if (inf == NULL) {
            logstream(LOG_FATAL) << "Could not load :" << filename << " error: " << strerror(errno) << std::endl;
        }
        assert(inf != NULL);

        mkdir((filename+"_invl/").c_str(), 0777);
        
        logstream(LOG_INFO) << "Reading in edge list format!" << std::endl;

        char * buf = (char*) malloc(invlsize*sizeof(int));
        char * bufptr = buf;
        
        char s[1024];
        invlid = 0;
        vid_t curvertex = 0;
        int count = 0;
        cursize = 0;
        std::vector<vid_t> outv;
        stv = env = 0;
        while(fgets(s, 1024, inf) != NULL) {
            if (s[0] == '#') continue; // Comment
            if (s[0] == '%') continue; // Comment
            
            char *t1, *t2;
            t1 = strtok(s, "\t, ");
            t2 = strtok(NULL, "\t, ");
            if (t1 == NULL || t2 == NULL ) {
                logstream(LOG_ERROR) << "Input file is not in right format. "
                << "Expecting \"<from>\t<to>\". "
                << "Current line: \"" << s << "\"\n";
                assert(false);
            }
            vid_t from = atoi(t1);
            vid_t to = atoi(t2);
            if( from == to ) continue;
            if( from == curvertex ){
                outv.push_back(to);
                count++;
            }else{
                bwrite( buf, bufptr, count, outv);
                if( from - curvertex > 1 ){ 
                    bwritezero( buf, bufptr, from-curvertex-1 ); 
        	        env += from - curvertex -1 ;
                }
                curvertex = from;
                count = 1;
                outv.clear();
                outv.push_back(to);     
            }
        }
        fclose(inf);
        std::string invlname = intervalname(filename, invlid);
        writefile(invlname, buf, bufptr);
        std::pair<vid_t, vid_t> invl(stv, env);
        invls.push_back(invl);
        std::cout << invlid << " " << stv << " " << env << std::endl;
        invlnum = invlid+1;
        std::cout << "Partitioned interval number : " << invlnum << std::endl;
        return invls;
    }
};