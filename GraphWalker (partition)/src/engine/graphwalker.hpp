
#ifndef DEF_GRAPHCHWALKER_ENGINE
#define DEF_GRAPHCHWALKER_ENGINE

#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <omp.h>
#include <vector>
#include <map>
#include <sys/time.h>

#include "api/filename.hpp"
#include "api/io.hpp"
#include "logger/logger.hpp"
#include "metrics/metrics.hpp"
#include "api/pthread_tools.hpp"
#include "walks/randomwalk.hpp"

class graphwalker_engine {
public:     
    std::string base_filename;
    int nblocks;  
    int nvertices;      
    size_t blocksize;
    int membudget_mb;
    int exec_threads;
    
    /* State */
    int exec_block;
    std::map<vid_t, int> imap;
    
    /* Metrics */
    metrics &m;

    /* --Rui */
    walkManager *walk_manager;
    int numIntervals;
        
    void print_config() {
        logstream(LOG_INFO) << "Engine configuration: " << std::endl;
        logstream(LOG_INFO) << " exec_threads = " << exec_threads << std::endl;
        // logstream(LOG_INFO) << " load_threads = " << load_threads << std::endl;
        logstream(LOG_INFO) << " membudget_mb = " << membudget_mb << std::endl;
        logstream(LOG_INFO) << " blocksize = " << blocksize << std::endl;
        // logstream(LOG_INFO) << " scheduler = " << use_selective_scheduling << std::endl;
    }
        
public:
        
    /**
     * Initialize GraphChi engine
     * @param base_filename prefix of the graph files
     * @param nshards number of shards
     * @param selective_scheduling if true, uses selective scheduling 
     */
    graphwalker_engine(std::string _base_filename, int _nblocks, metrics &_m) : base_filename(_base_filename), nblocks(_nblocks), m(_m) {

        membudget_mb = get_option_int("membudget_mb", 1024);
        exec_threads = get_option_int("execthreads", omp_get_max_threads());

        walk_manager = new walkManager(m);
        walk_manager->initialnizeWalks(nblocks, nvertices, base_filename);
    }
        
    virtual ~graphwalker_engine() {
    }

    void loadBlock(int p, std::vector<Vertex> &vertices ){
        std::string bname = blockname( base_filename, p );
        int inf = open(bname.c_str(),O_RDONLY | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        if (inf < 0) {
          logstream(LOG_FATAL) << "Could not load :" << base_filename << " error: " << strerror(errno) << std::endl;
        }
        assert(inf > 0);
        char * buf;
        size_t sz = readfull(inf, &buf);
        char * bufptr = buf;
        int vcnt = sz / sizeof(int);
        int cnt = 0;
        vertices.clear();
        imap.clear();
        while( vcnt > 0 ){
          Vertex v;
          v.vid = *((int*)bufptr);
          bufptr += sizeof(int);
          int dcnt = *((int*)bufptr);
          bufptr += sizeof(int);
          v.outd = dcnt;
          for( int i = 0; i < v.outd; i++ ){
              vid_t to = *((vid_t*)bufptr);
              bufptr += sizeof(vid_t);
              v.outv.push_back(to);
          }
          vertices.push_back(v);
          imap[v.vid] = cnt;
          cnt++;
          vcnt -= (v.outd + 2);
        }
        free(buf);
        close(inf);
    }

    virtual void exec_updates(RandomWalkProgram &userprogram, std::vector<Vertex> &vertices) {
        omp_set_num_threads(exec_threads);
        unsigned nv = (unsigned)(vertices.size());
        #pragma omp parallel for
            for( unsigned i = 0; i < nv; i++ ){
                userprogram.updateByWalk(vertices, i, exec_block, imap, *walk_manager);
            }
    }

    void walkToEnd(RandomWalkProgram &userprogram, std::vector<Vertex> &vertices){
        exec_updates(userprogram, vertices);
    }

    void runInterval(RandomWalkProgram &userprogram){
        /* Load data */
        std::vector<Vertex> vertices;
        loadBlock(exec_block, vertices);
        walkToEnd(userprogram, vertices);
    }

    void loadOnDemand(RandomWalkProgram &userprogram){

        /* Interval loop */
        numIntervals = 0;
        /* -- move walks -- Rui */
        while( walk_manager->notFinish() ){
            exec_block =walk_manager->intervalWithMaxWalks();
            //walk_manager->printWalksDistribution( exec_interval );
            metrics_entry mr = m.start_time();
            runInterval(userprogram);
            m.stop_time(mr, "_in_run_interval");
        } // For exec_interval
    }

    void run(RandomWalkProgram &userprogram) {
        // initialize_before_run();
        userprogram.startWalks(*walk_manager);
        loadOnDemand(userprogram);
    }
};

#endif