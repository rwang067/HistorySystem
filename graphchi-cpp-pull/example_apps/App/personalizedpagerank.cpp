

/**
 * @file
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
 *
 * @section DESCRIPTION
 *
 * Random walk simulation. From a set of source vertices, a set of 
 * random walks is started. Random walks walk via edges, and we use the
 * dynamic chivectors to support multiple walks in one edge. Each
 * vertex keeps track of the walks that pass by it, thus in the end
 * we have estimate of the "pagerank" of each vertex.
 *
 * Note, this version does not support 'resets' of random walks.
 * TODO: from each vertex, start new random walks with some probability,
 * and also terminate a walk with some probablity.
 *
 */

#define DYNAMICEDATA 1

#include <string>
#include <vector>

#include "graphchi_basic_includes.hpp"
#include "api/dynamicdata/chivector.hpp"
#include "util/toplist.hpp"

using namespace graphchi;

/**
 * Type definitions. Remember to create suitable graph shards using the
 * Sharder-program.
 */
typedef unsigned VertexDataType;
typedef chivector<vid_t>  EdgeDataType;

 
struct PPVProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {

public:
    vid_t s;
    int N;
    unsigned R;
    unsigned L;
    std::vector<unsigned> restart;
    
public:
    int walks_per_source() {
        // return 1667;
        // return 3167;
        // return R+R*L*0.15;
        return R;
    }
    
    bool is_source(vid_t v) {
        // return (v % 50 == 0);
        return (v == s );
    }
    
    /**
     *  Vertex update function.
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType > &vertex, graphchi_context &gcontext) {
        if (gcontext.iteration == 0) {
            
            if (is_source(vertex.id())) {
                for(int i=0; i < walks_per_source(); i++) {
                    /* Get random out edge's vector */
                    graphchi_edge<EdgeDataType> * outedge = vertex.random_outedge();
                    if (outedge != NULL) {
                        chivector<vid_t> * evector = outedge->get_vector();
                        /* Add a random walk particle, represented by the vertex-id of the source (this vertex) */
                        unsigned walk = (( vertex.id() & 0x3ffff ) << 14 ) | ( i & 0x3fff ) ;
                        evector->add(walk);
                        gcontext.scheduler->add_task(outedge->vertex_id()); // Schedule destination
                    }
                }
                vertex.set_data(0);
            }else{
                vertex.set_data(0);
            }
        } else {
            int num_walks = 0;
            if (is_source(vertex.id())) {
                for( unsigned i = 0; i < restart.size(); i++ ){
                    unsigned walk = restart[i];
                    if( (walk & 0x3fff) < L ){
                        walk++;
                        graphchi_edge<EdgeDataType> * outedge = vertex.random_outedge();
                        float cc = ((float)rand())/RAND_MAX;
                        if (outedge != NULL && cc > 0.15 ) {
                            chivector<vid_t> * evector = outedge->get_vector();
                             //Add a random walk particle, represented by the vertex-id of the source (this vertex) 
                            evector->add(walk);
                            gcontext.scheduler->add_task(outedge->vertex_id()); // Schedule destination
                        }
                    }else{
                        vertex.set_data(vertex.get_data() + 1);
                    }
                }
                num_walks += restart.size();
                restart.clear();
            }
            /* Check inbound edges for walks and advance them. */
            for(int i=0; i < vertex.num_inedges(); i++) {
                graphchi_edge<EdgeDataType> * edge = vertex.inedge(i);
                chivector<vid_t> * invector = edge->get_vector();
                for(int j=0; j < invector->size(); j++) {
                    /* Get one walk */
                    vid_t walk = invector->get(j);
                    if( (walk & 0x3fff) < L ){
                        walk++;
                        /* Move to a random out-edge */
                        graphchi_edge<EdgeDataType> * outedge = vertex.random_outedge();
                        float cc = ((float)rand())/RAND_MAX;
                        if (outedge != NULL && cc > 0.15 ) {
                            chivector<vid_t> * outvector = outedge->get_vector();
                            /* Add a random walk particle, represented by the vertex-id of the source (this vertex) */
                            outvector->add(walk);
                            gcontext.scheduler->add_task(outedge->vertex_id()); // Schedule destination
                        }else{
                            restart.push_back(walk);//jump back to source
                        }
                    }else{
                        vertex.set_data(vertex.get_data() + 1);
                    }
                    num_walks ++;
            }
                /* Remove all walks from the inbound vector */
                invector->clear();
            }
            /* Keep track of the walks passed by via this vertex */
            // std::cout << "--------------vertex vertex.get_data(), num_walks : " << vertex.id() << " " << vertex.get_data() <<" " << num_walks << std::endl;
            // vertex.set_data(vertex.get_data() + num_walks);
        }
    }

    void writeFile(std::string basefilename){
        // read the vertex value
        unsigned *vertex_value = (unsigned*)malloc(N*sizeof(unsigned));
        // std::string vertex_value_file = filename_vertex_data(basefilename);
        std::string vertex_value_file = basefilename + "_GraphChi/4B.vout";
        int f1 = open(vertex_value_file.c_str(), O_RDONLY, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        if (f1 < 0) {
            logstream(LOG_ERROR) << "Could not open " << vertex_value_file << " error: " << strerror(errno) << std::endl;
        }
        assert(f1 >= 0);
        preada(f1, vertex_value, N*sizeof(float), 0);
        close(f1);
        //compute the probability
         unsigned sum = 0;
        for( int i = 0; i < N; i++ ){
            if(vertex_value[i] > R )
                std::cout << "i, vertex_value : " << i << " " << vertex_value[i] << std::endl;
            sum += vertex_value[i];
        }
        std::cout << "sum : " << sum << " " << R*L << std::endl;
        float *visit_prob = (float*)malloc(N*sizeof(float));
        for( int i = 0; i < N; i++ ){
            visit_prob[i] = vertex_value[i] * 1.0 / sum;
        }

        int f = open(vertex_value_file.c_str(), O_WRONLY | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        if (f < 0) {
            logstream(LOG_ERROR) << "Could not open " << vertex_value_file << " error: " << strerror(errno) << std::endl;
        }
        assert(f >= 0);
        pwritea(f, visit_prob, N*sizeof(float), 0);
        close(f);
        free(vertex_value);
        free(visit_prob);
    }
    
};

 

int main(int argc, const char ** argv) {
    /* GraphChi initialization will read the command line
     arguments and the configuration file. */
    graphchi_init(argc, argv);
    
    /* Metrics object for keeping track of performance counters
     and other information. Currently required. */
    metrics m("randomwalk");
    
    /* Basic arguments for application */
    std::string filename = get_option_string("file", "/home/wang/Documents/graph processing system/dataset/LiveJournal1/soc-LiveJournal1.txt");  // Base filename
    int nvertices           = get_option_int("nvertices", 4847571); // Number of iterations
    int source           = get_option_int("source", 0); // 
    int L           = get_option_int("L", 20); // Number of iterations
    int R           = get_option_int("R", 100000); // 
    bool scheduler       = true;                       // Whether to use selective scheduling
    
    /* Detect the number of shards or preprocess an input to create them */
    bool preexisting_shards;
    int nshards          = convert_if_notexists<vid_t>(filename, get_option_string("nshards", "auto"), preexisting_shards);
    
    /* Run */
    PPVProgram program;
    program.s = (vid_t)source;
    program.N = nvertices;
    program.R = R;
    program.L = L;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);
    if (preexisting_shards) {
        engine.reinitialize_edge_data(0);
    }
    engine.run(program, L);
    program.writeFile(filename);
    
    /* List top 20 */
    int ntop = 50;
    std::vector< vertex_value<float> > top = get_top_vertices<float>(filename, ntop);
    std::cout << "Print top 20 vertices: " << std::endl;
    for(int i=0; i < (int) top.size(); i++) {
        std::cout << (i+1) << ". " << top[i].vertex << "\t" << top[i].value << std::endl;
    }

    // read the accurate value
        float *ppv = (float*)malloc(nvertices*sizeof(float));
        std::string PPV_file = "/home/wang/Documents/graph processing system/dataset/LiveJournal1/PPR0.vout";
        int f1 = open(PPV_file.c_str(), O_RDONLY, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        if (f1 < 0) {
            logstream(LOG_ERROR) << "Could not open " << PPV_file << " error: " << strerror(errno) << std::endl;
        }
        assert(f1 >= 0);
        preada(f1, ppv, nvertices*sizeof(float), 0);
        close(f1);
        //compute the error
        float err = 0;
        for( int i = 0; i < ntop; i++ ){
            err += fabs(top[i].value - ppv[top[i].vertex])/ppv[top[i].vertex];//(ppv[i]-visit_prob[i])*(ppv[i]-visit_prob[i]);
        }
        err = err / ntop;
        std::cout << "Error : " << err << std::endl;

        std::ofstream errfile;
        errfile.open("/home/wang/Documents/graph processing system/dataset/LiveJournal1/ppv0.error", std::ofstream::app);
        errfile << err << "\n" ;
        errfile.close();
        free(ppv);

    /* Report execution metrics */
    metrics_report(m);
    return 0;
}
