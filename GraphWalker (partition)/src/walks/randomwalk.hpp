#ifndef RANDOMWALKPROGRAM
#define RANDOMWALKPROGRAM

#include <string>
#include <fstream>
#include <time.h>

#include "walks/walk.hpp" 
#include "api/datatype.hpp"

/**
 * Type definitions. Remember to create suitable graph shards using the
 * Sharder-program.
 */
 
class RandomWalkProgram {

private:
    int nwalks; 
    int nsteps;
    int nvertices;
    float boundRatio,  intervalBoundRatio;
    char *bidx;

public:
    int degree;
    int count;
    void initialization( int nv, int nw, int ns, float rb, float rbi, char *idx ) {
        nvertices = nv;
        nwalks = nw;
        nsteps = ns;
        boundRatio = rb;
        intervalBoundRatio = rbi;
        std::cout << nv << " " << nw << " " << ns << " " << rb << " " << rbi << std::endl;
        bidx = idx;
    }

    void startWalks( walkManager &walk_manager ){
        
        int startWalksNum = nwalks + nwalks*boundRatio;
        int stopWalksNum = nwalks*boundRatio;
        walk_manager.getWalkNum(nwalks, stopWalksNum);

        for( unsigned i = 0; i < walk_manager.walkscnt.size(); i++ ){
            walk_manager.walkscnt[i] = 0;
        }

        srand((unsigned)time(NULL));
        for( int i = 0; i < startWalksNum; i++ ){
            vid_t s = rand() % nvertices;
            WalkDataType walk = walk_manager.encode(s, 0);
            walk_manager.addWalk(s, walk );
            int p = (int)*(bidx+s);
            walk_manager.walkscnt[p]++;
        }
        degree = 0;
        count = 0;
    }
    
    /**
     *  Walk update function.
     */
    void updateByWalk(std::vector<Vertex> &vertices, int x, int curblock, std::map<vid_t, int> imap, walkManager &walk_manager){
        Vertex vertex = vertices[x];
        while (!walk_manager.emptyWalk(vertex.vid)){
            vid_t dstId = vertex.vid;
            int p = (int)*(bidx+dstId);
            while (p == curblock ){
                int y = imap[dstId];
                Vertex &nowVertex = vertices[y];
                WalkDataType nowWalk = walk_manager.getWalk(nowVertex.vid);
                if (walk_manager.getHop(nowWalk) >= nsteps)
                    break;
                degree += nowVertex.outd;
                count++;
                //nowVertex.set_data(nowVertex.get_data()+1);
                if ( vertex.outd > 0 ) {
                    vid_t v = random_outneighbor(vertex);
                    dstId = v;
                }
                else{
                    dstId = rand() % nvertices;
                }
                walk_manager.moveWalk(nowWalk, dstId);
                p = (int)*(bidx+dstId);
            }
            walk_manager.walkscnt[p]++;
        }
    }
    
    /**
     * Called before an execution interval is started.
     */
    void before_exec_interval() {
        //
    }
    
    /**
     * Called after an execution interval has finished.
     */
    void after_exec_interval() {
        //
    }
    
};

#endif