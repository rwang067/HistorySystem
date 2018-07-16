
#define DYNAMICEDATA 1

#include <string>
#include <fstream>

#include "graphchi_basic_includes.hpp"
#include "walks/randomwalk.hpp"
#include "api/dynamicdata/chivector.hpp"
#include "util/toplist.hpp"

using namespace graphchi;

int main(int argc, const char ** argv) {
    /* GraphChi initialization will read the command line
     arguments and the configuration file. */
    graphchi_init(argc, argv);
    
    /* Metrics object for keeping track of performance count_invectorers
     and other information. Currently required. */
    metrics m("randomwalk");
    
    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    bool scheduler       = true;                       // Whether to use selective scheduling

    int nvertices = get_option_int("nvertices", 0); // Number of vertices
    int nwalks = get_option_int("nwalks", 0); // Number of walks
    int nsteps = get_option_int("nsteps", 0); // Number of steps
    
    /* Detect the number of shards or preprocess an input to create them */
    bool preexisting_shards;
    int nshards          = convert_if_notexists<vid_t>(filename, get_option_string("nshards", "auto"), preexisting_shards);

    /* Run */
    RandomWalkProgram program;
    program.initialization( nvertices, nwalks, nsteps );
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);
    if (preexisting_shards) {
        // engine.reinitialize_edge_data(0);
        engine.reinitialize_vertex_data(0);
    }
    engine.run(program, nsteps);
    
    /* List top 20 */
    /*int ntop = 20;
    std::vector< vertex_value<VertexDataType> > top = get_top_vertices<VertexDataType>(filename, ntop);
    std::cout << "Print top 20 vertices: " << std::endl;
    for(int i=0; i < (int) top.size(); i++) {
        std::cout << (i+1) << ". " << top[i].vertex << "\t" << top[i].value << std::endl;
    }*/
    // std::cout << "average degree : " << program.count << " " << program.degree*1.0/program.count << std::endl;

    /* Report execution metrics */
    metrics_report(m);
    return 0;
}