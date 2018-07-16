
#define DYNAMICEDATA 1

#include <string>
#include <fstream>

#include <time.h>

#include "graphchi_basic_includes.hpp"
#include "api/dynamicdata/chivector.hpp"
#include "walks/walk.hpp"   // -Rui
#include "util/toplist.hpp"
#include "metrics/metrics.hpp"

using namespace graphchi;

/**
 * Type definitions. Remember to create suitable graph shards using the
 * Sharder-program.
 */
typedef unsigned int VertexDataType;
typedef chivector<vid_t>  EdgeDataType;

 
class RandomWalkProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {

private:
    int nwalks; 
    int nsteps;
    int nvertices;
    float boundRatio,  intervalBoundRatio;

public:
/*    int degree;
    int count;*/
    void initialization( int nv, int nw, int ns, float rb ) {
        nvertices = nv;
        nwalks = nw;
        nsteps = ns;
        boundRatio = rb;
        std::cout << nv << " " << nw << " " << ns << " " << rb << std::endl;
    }

    void startWalks( walkManager &walk_manager ){
        
        int startWalksNum = nwalks + nwalks*boundRatio;
        int stopWalksNum = nwalks*boundRatio;
        std::cout << startWalksNum << " " << stopWalksNum << std::endl;
        walk_manager.getWalkNum(nwalks, stopWalksNum);

        srand((unsigned)time(NULL));
        for( int i = 0; i < startWalksNum; i++ ){
            vid_t s = rand() % nvertices;
            WalkDataType walk = walk_manager.encode(s, 0);
            walk_manager.addWalk(s, walk );
            // std::cout << "s     " << s << " " <<  i  << " " << nvertices << std::endl;
            // std::cout << walk << "  " << walk_manager.getSourceId(walk) << " " << walk_manager.getHop(walk) << std::endl;
        }
/*        degree = 0;
        count = 0;*/
    }
    
    /**
     *  Vertex update function.
     */
    void updateByVertex(graphchi_vertex<VertexDataType, EdgeDataType > &vertex, walkManager &walk_manager, 
                                                                                                                graphchi_context &gcontext) {
        // int num_walks = 0;
        while( !walk_manager.emptyWalk( vertex.id() ) ){
            WalkDataType walk = walk_manager.getWalk( vertex.id() );
            if( walk_manager.getHop(walk) <  nsteps ){
                if( walk_manager.getHop(walk) > 0 )
                #pragma omp critical
                    {
                        vertex.set_data(vertex.get_data() + 1 );
                    }
                /* Move to a random out-edge */
                graphchi_edge<EdgeDataType> * outedge = vertex.random_outedge();
                if (outedge != NULL) {
                // Rui adds critical 
                #pragma omp critical
                {
                    walk_manager.moveWalk( walk, outedge->vertex_id());
                     // std::cout << vertex.id() << "    ->    " << outedge->vertex_id() << std::endl;
                }
                }else{ // sink node
                    vid_t s = rand() % nvertices;
                    // Rui adds critical
                    #pragma omp critical
                    {
                        walk_manager.moveWalk( walk, s );
                    }
                    // walk_manager.moveWalk( walk, walk_manager.getSourceId(walk) );
                    // std::cout << walk << "  " << walk_manager.getSourceId(walk) << " " << walk_manager.getHop(walk) << std::endl;
                    // std::cout << vertex.id() << "    ->    " << walk_manager.getSourceId(walk) << std::endl;
                }
                /*degree += vertex.num_inedges() + vertex.num_outedges();
                count++;
                num_walks ++;*/
            }else{
                // degree += vertex.num_inedges() + vertex.num_outedges();
                // count++;
                // Rui adds critical
                #pragma omp critical
                {
                    vertex.set_data(vertex.get_data() + 1 );
                }
            }
        }
        // vertex.set_data(vertex.get_data() + num_walks);
        // if( vertex.get_data() != 0 ) std::cout << vertex.id() << " :     " << vertex.get_data() << std::endl;
    }

    /*void updateByWalk(std::vector<graphchi_vertex<VertexDataType, EdgeDataType> > &vertices, vid_t vid, int sub_interval_st, int sub_interval_en, walkManager &walk_manager, graphchi_context &gcontext){
        graphchi_vertex<VertexDataType, EdgeDataType> &vertex = vertices[vid - sub_interval_st];
        while (!walk_manager.emptyWalk(vertex.id())){
            vid_t dstId = vertex.id();
            while (dstId >= (vid_t)sub_interval_st && dstId < (vid_t)sub_interval_en){
                std::cout << vertex.id() << " " << dstId << std::endl;
                graphchi_vertex<VertexDataType, EdgeDataType> &nowVertex = vertices[dstId-sub_interval_st];
                WalkDataType nowWalk = walk_manager.getWalk(nowVertex.id());
                if (walk_manager.getHop(nowWalk) >= nsteps)
                    break;
                degree += nowVertex.num_edges();
                count++;
                //nowVertex.set_data(nowVertex.get_data()+1);
                graphchi_edge<EdgeDataType> * outedge = nowVertex.random_outedge();
                if (outedge != NULL)
                    dstId = outedge -> vertex_id();
                else
                    dstId = rand() % nvertices;
                walk_manager.moveWalk(nowWalk, dstId);
            }
        }
    }*/

    void updateByWalk(std::vector<graphchi_vertex<VertexDataType, EdgeDataType> > &vertices, vid_t vid, int sub_interval_st, int sub_interval_en, walkManager &walk_manager, graphchi_context &gcontext){
        // std::cout << "update by walk start" << std::endl;
        graphchi_vertex<VertexDataType, EdgeDataType> &vertex = vertices[vid - sub_interval_st];
        while (!walk_manager.emptyWalk(vertex.id())){
            WalkDataType nowWalk = walk_manager.getWalk(vertex.id());
             // std::cout << vertex.id() << " : walk " << nowWalk << std::endl;
            int hop = walk_manager.getHop(nowWalk);
            // std::cout << vertex.id() << " : hop" << hop << std::endl;
            vid_t dstId = vertex.id();
            while (dstId >= (vid_t)sub_interval_st && dstId <= (vid_t)sub_interval_en && hop < nsteps ){
                // std::cout << vertex.id() << " " << dstId << std::endl;
                graphchi_vertex<VertexDataType, EdgeDataType> &nowVertex = vertices[dstId-sub_interval_st];
                 if( hop > 0 )
                 #pragma omp critical
                    {
                        nowVertex.set_data(vertex.get_data() + 1 );
                    }
                /*degree += nowVertex.num_edges();
                count++;*/
                //nowVertex.set_data(nowVertex.get_data()+1);
                graphchi_edge<EdgeDataType> * outedge = nowVertex.random_outedge();
                if (outedge != NULL)
                    dstId = outedge -> vertex_id();
                else
                    dstId = rand() % nvertices;
                hop++;
            }
            if( hop < nsteps  ){
                walk_manager.moveWalktoHop(nowWalk, dstId, hop);
                if( dstId >= (vid_t)sub_interval_st && dstId <= (vid_t)sub_interval_en)
                    std::cout << vertex.id() << " move walk " << nowWalk << "  " << dstId << "  " << hop << std::endl;
            }
        }
        // std::cout << "update by walk end" << std::endl;
    }
    
    /**
     * Called before an execution interval is started.
     */
    void before_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &gcontext) {
        //
    }
    
    /**
     * Called after an execution interval has finished.
     */
    void after_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &gcontext) {
        //
    }
    
};