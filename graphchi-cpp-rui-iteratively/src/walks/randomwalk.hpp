
#define DYNAMICEDATA 1

#include <string>
#include <fstream>

#include <time.h>

#include "graphchi_basic_includes.hpp"
#include "api/dynamicdata/chivector.hpp"
#include "walks/walk.hpp"   // -Rui
#include "util/toplist.hpp"

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

public:
    int degree;
    int count;
    void initialization( int nv, int nw, int ns ) {
        nvertices = nv;
        nwalks = nw;
        nsteps = ns;
    }

    void startWalks( walkManager &walk_manager ){
        srand((unsigned)time(NULL));
        for( int i = 0; i < nwalks; i++ ){
            vid_t s = rand() % nvertices;
            WalkDataType walk = walk_manager.encode(s, 0);
            walk_manager.addWalk(s, walk );
            // std::cout << "s     " << s << " " <<  i  << " " << nvertices << std::endl;
            // std::cout << walk << "  " << walk_manager.getSourceId(walk) << " " << walk_manager.getHop(walk) << std::endl;
        }
        degree = 0;
        count = 0;
    }
    
    /**
     *  Vertex update function.
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType > &vertex, walkManager &walk_manager, graphchi_context &gcontext) {
        int num_walks = 0;
        while( !walk_manager.emptyWalk( vertex.id() ) ){
            WalkDataType walk = walk_manager.getWalk( vertex.id() );
            if( walk_manager.getHop(walk) <  nsteps ){
                /* Move to a random out-edge */
                graphchi_edge<EdgeDataType> * outedge = vertex.random_outedge();
                if (outedge != NULL) {
                    walk_manager.moveWalk( walk, outedge->vertex_id());
                    gcontext.scheduler->add_task(outedge->vertex_id()); // Schedule destination
                     // std::cout << vertex.id() << "    ->    " << outedge->vertex_id() << std::endl;
                }else{ // sink node
                    vid_t s = rand() % nvertices;
                    walk_manager.moveWalk( walk, s );
                    gcontext.scheduler->add_task(s); // Schedule destination
                    // walk_manager.moveWalk( walk, walk_manager.getSourceId(walk) );
                    // std::cout << walk << "  " << walk_manager.getSourceId(walk) << " " << walk_manager.getHop(walk) << std::endl;
                    // std::cout << vertex.id() << "    ->    " << walk_manager.getSourceId(walk) << std::endl;
                }
                degree += vertex.num_inedges() + vertex.num_outedges();
                count++;
                num_walks ++;
            }/*else{
                degree += vertex.num_inedges() + vertex.num_outedges();
                count++;
            }*/
        }
        vertex.set_data(vertex.get_data() + num_walks);
        // if( vertex.get_data() != 0 ) std::cout << vertex.id() << " :     " << vertex.get_data() << std::endl;
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