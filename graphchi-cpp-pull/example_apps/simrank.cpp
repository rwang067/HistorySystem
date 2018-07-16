
#define DYNAMICEDATA 1

#include <string>

#include "graphchi_basic_includes.hpp"
#include "api/dynamicdata/chivector.hpp"
#include "util/toplist.hpp"

using namespace graphchi;

/**
 * Type definitions. Remember to create suitable graph shards using the
 * Sharder-program.
 */
typedef unsigned int VertexDataType;
typedef chivector<vid_t>  EdgeDataType;

class SimRank : public GraphChiProgram<VertexDataType, EdgeDataType>{
private:
	vid_t a, b;
	unsigned R, L;
	std::vector<vid_t> walksfroma;
	std::vector<vid_t> walksfromb;

public:
	void initializeApp( vid_t _a, vid_t _b, unsigned _R, unsigned _L ){
		a = _a;
		b = _b;
		R = _R;
		L = _L;
		walksfroma.resize(R*L);
		walksfromb.resize(R*L);
		memset(walksfroma.data(), 0xffffffff, walksfroma.size()*sizeof(vid_t));
		memset(walksfromb.data(), 0xffffffff, walksfromb.size()*sizeof(vid_t));
	}

	void updateInfo(unsigned walk, unsigned l, vid_t dstId){
		vid_t s = (vid_t)( walk >> 12 ) & 0xfffff;
		unsigned r = (unsigned)(walk & 0xfff) * L + l;
		// std::cout << "update : " << s << " " << (unsigned)(walk & 0xfff) << " " << l << " " << dstId << " " << r << std::endl;
		if( s == a )
			walksfroma[r] = dstId;
		else if( s == b )
			walksfromb[r] = dstId;
		else{
			logstream(LOG_ERROR) << "Wrong source s : " << s << std::endl;
			assert(0);
		}
		// std::cout << "s , r, dstId, walk = " << s << " " << r << " " << dstId << " " << walk << std::endl;
	}

	/**
     *  Vertex update function.
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType > &vertex, graphchi_context &gcontext) {
        if (gcontext.iteration == 0) {
            
            if ( vertex.id() == a ||  vertex.id() == b ) {
                for(unsigned i = 0; i < R; i++) {
                    /* Get random out edge's vector */
                    graphchi_edge<EdgeDataType> * outedge = vertex.random_outedge();
                    if (outedge != NULL) {
                    	// std::cout << vertex.id() << " " << i << " " << outedge->vertex_id() << std::endl;
                        chivector<vid_t> * evector = outedge->get_vector();
                        /* Add a random walk particle, represented by the vertex-id of the source (this vertex) */
                        unsigned walk = (( vertex.id() & 0xfffff ) << 12 ) | ( i & 0xfff ) ;
                        evector->add(walk);
                        gcontext.scheduler->add_task(outedge->vertex_id()); // Schedule destination
                		updateInfo(walk, 0, outedge->vertex_id() );
                    }
                }
            }
        } else {
            /* Check inbound edges for walks and advance them. */
            int num_walks = 0;
            for(int i=0; i < vertex.num_inedges(); i++) {
                graphchi_edge<EdgeDataType> * edge = vertex.inedge(i);
                chivector<vid_t> * invector = edge->get_vector();
                for(int j=0; j < invector->size(); j++) {
                    /* Get one walk */
                    vid_t walk = invector->get(j);
                    /* Move to a random out-edge */
                    graphchi_edge<EdgeDataType> * outedge = vertex.random_outedge();
                    if (outedge != NULL) {
                        chivector<vid_t> * outvector = outedge->get_vector();
                        /* Add a random walk particle, represented by the vertex-id of the source (this vertex) */
                        outvector->add(walk);
                        gcontext.scheduler->add_task(outedge->vertex_id()); // Schedule destination
                        // std::cout << vertex.id() << " " << i << " " << j << " " << walk << std::endl;
                        updateInfo(walk, gcontext.iteration, outedge->vertex_id() );
                    }
                    num_walks ++;
                }
                /* Remove all walks from the inbound vector */
                invector->clear();
            }
            /* Keep track of the walks passed by via this vertex */
        }
    }

	float computeResult(){
		float simrank = 0;
		for( unsigned i = 0; i < R; i++ )
			for( unsigned j = 0; j < R; j++ )
				for( unsigned l = 0; l < L; l++ ){
					// std::cout << i << " " << j << " " << l << " : " << walksfroma[i*L+l] << " " << walksfromb[j*R+l] << std::endl;
					if( walksfroma[i*L+l] == walksfromb[j*L+l] && walksfroma[i*L+l] != 0xffffffff ){
						simrank += (1.0/(R*R))*pow(0.8, l);
						std::cout << i << " " << j << " " << l << " " << walksfroma[i*L+l] << " " << simrank << std::endl;
						break;
					}
				}
		return simrank;
	}

};

int main(int argc, const char ** argv) {
    /* Read the command line arguments and the configuration file. */
    graphchi_init(argc, argv);
    /* Metrics object for keeping track of performance count_invectorers and other information. Currently required. */
    metrics m("simrank");
    
    /* Basic arguments for application */
    std::string filename = get_option_string("file", "/home/wang/Documents/graph processing system/dataset/LiveJournal1/soc-LiveJournal1.txt");  // Base filename
    int a = get_option_int("a", 1); // 
    int b = get_option_int("b", 2); // 
    int R = get_option_int("R", 100); // Number of walks
    int L = get_option_int("L", 10); // Number of steps per walk
    
    /* Detect the number of shards or preprocess an input to create them */
    bool scheduler       = true;    
    bool preexisting_shards;
    int nshards          = convert_if_notexists<vid_t>(filename, get_option_string("nshards", "auto"), preexisting_shards);

    /* Run */
    SimRank program;
    program.initializeApp( a, b, R, L );
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);
    if (preexisting_shards) {
        engine.reinitialize_edge_data(0);
    }
    engine.run(program, L);

    float simrank = program.computeResult();
    std::cout << "SimRank for " << a << " and " << b << " = " << simrank << std::endl;
    
    /* Report execution metrics */
    metrics_report(m);
    return 0;
}