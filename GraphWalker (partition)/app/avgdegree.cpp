
#define DYNAMICEDATA 1

#include <string>
#include <fstream>

#include "api/graphwalker_basic_includes.hpp"

int main(int argc, const char ** argv) {
    /* GraphChi initialization will read the command line
     arguments and the configuration file. */
    set_argc(argc, argv);
    
    /* Metrics object for keeping track of performance count_invectorers
     and other information. Currently required. */
    metrics m("randomwalk");
    
    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    int nvertices = get_option_int("nvertices"); // Number of vertices
    int nwalks = get_option_int("nwalks"); // Number of walks
    int nsteps = get_option_int("nsteps"); // Number of steps
    float rbound = get_option_float("rbound", 0); // Ratio of lower bound  of stop walks
    float rboundin = get_option_float("rboundin", 0); // Ratio of lower bound  of interval stop walks
    
    /* Detect the number of shards or preprocess an input to create them */
    /*bool preexisting_shards;
    int nshards = convert_if_notexists<vid_t>(filename, get_option_string("nshards", "auto"), preexisting_shards);*/
    char *bidx = (char*) malloc(nvertices*sizeof(char));
    BfsPartition bfs_partition_obj(filename);
    int nblocks = bfs_partition_obj.partition(&bidx);

    /* Run */
    RandomWalkProgram program;
    program.initialization( nvertices, nwalks, nsteps, rbound, rboundin, bidx );
    graphwalker_engine engine(filename, nblocks, m);
    engine.run(program);
    free(bidx);
    
    /* List top 20 */
    /*int ntop = 20;
    std::vector< vertex_value<VertexDataType> > top = get_top_vertices<VertexDataType>(filename, ntop);
    std::cout << "Print top 20 vertices: " << std::endl;
    for(int i=0; i < (int) top.size(); i++) {
        std::cout << (i+1) << ". " << top[i].vertex << "\t" << top[i].value << std::endl;
    }*/

    /*int sum = 0;
     for(int i=0; i < (int) top.size(); i++) {
        sum += top[i].value;
     }
     int deg = 0;
     for(int i=0; i < (int) top.size(); i++) {
        deg += top[i].value * top[i].value;
     }*/
    std::cout << "average degree : " << program.count << " " << program.degree*1.0/program.count << std::endl;

    /* Report execution metrics */
    metrics_report(m);
    return 0;
}