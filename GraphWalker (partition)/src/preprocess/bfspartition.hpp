
#ifndef BFSPARTITION
#define BFSPARTITION

#include <queue>

#include "preprocess/simpartition.hpp"

class BfsPartition
{
private:
    std::string filename;
    int nvertices;
    std::vector<std::pair<vid_t, vid_t>> invls;
    int invlnum;
    int curinvl;
    int invlsize;
    int blockid;
    int blocksize;
    int cursize;
    int membudget_mb;
    std::vector<Vertex> vertices;
    bool *visited;
public:
    BfsPartition(std::string inputfile){
        filename = inputfile;
    };
    ~BfsPartition(){};

    void computeBlocksize(){
        membudget_mb = 800;
        invlsize = 10*1024 * 1024 ;
        blocksize  = 10*1024 * 1024 ;
    }

    int findInvl( vid_t u ){
        for( int i = 0; i < invlnum; i++)
            if( u <= invls[i].second )
                return i;
        return -1;
    }

    int find_partition(char *bidx){
        int p = 0;
        std::string bname = blockname( filename, p );
        int inf = open(bname.c_str(),O_RDONLY | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        while (inf > 0) {
            char * buf;
            size_t sz = readfull(inf, &buf);
            char * bufptr = buf;
            int vcnt = sz / sizeof(int);
            int cnt = 0;
            while( vcnt > 0 ){
                vid_t vv = *((int*)bufptr);
                *(bidx+vv) = (char)p;
                bufptr += sizeof(int);
                int dcnt = *((int*)bufptr);
                bufptr += sizeof(int);
                bufptr += sizeof(vid_t)*dcnt;
                cnt++;
                vcnt -= (dcnt + 2);
            }
        free(buf);
        close(inf);
        bname = blockname( filename, ++p );
        inf = open(bname.c_str(),O_RDONLY | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        }
        close(inf);
        return p;
    }

    void loadInvl(int p){
        // std::cout << " load interval " << p << " start ." << std::endl; 
        std::string invlname = intervalname( filename, p );
        int inf = open(invlname.c_str(),O_RDONLY | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
        if (inf < 0) {
            logstream(LOG_FATAL) << "Could not load :" << filename << " error: " << strerror(errno) << std::endl;
        }
        assert(inf > 0);
        char * buf;// = (char*) malloc(invlsize*sizeof(int));
        size_t sz = readfull(inf, &buf);
        char * bufptr = buf;
        int vcnt = sz / sizeof(int);
        int curvertex = invls[p].first;
        vertices.clear();
        while( vcnt > 0 ){
            Vertex v;
            v.vid = curvertex;
            int dcnt = *((int*)bufptr);
            // if( curvertex < 10 ) std::cout << curvertex << " d: " << *((int*)bufptr) << " : ";
            bufptr += sizeof(int);
            v.outd = dcnt;
            for( int i = 0; i < v.outd; i++ ){
                vid_t to = *((vid_t*)bufptr);
                // if( curvertex < 10 ) std::cout << *((int*)bufptr) << "  ";
                bufptr += sizeof(vid_t);
                v.outv.push_back(to);
            }
            // if( curvertex < 10 )  std::cout << std::endl;
            vertices.push_back(v);
            curvertex++;
            vcnt -= (v.outd + 1);
            /*if( p == 1 ){
                for( unsigned i = 0; i < vertices.size(); i++ ){
                    std::cout << vertices[i].vid << " " << vertices[i].outd << " : " << std::endl;
                    for( int j = 0; j < vertices[i].outd; j++ )
                        std::cout << vertices[i].outv[j] << " ";
                    std::cout << std::endl;
                }
            }*/
        }
        free(buf);
        close(inf);
        // std::cout << " load interval " << p << " end ." << std::endl; 
        // std::cout << p << "  " << vertices.size() << std::endl; 
    }

    bool add_vertex(char * buf, char * &bufptr, Vertex v){
        if( cursize + v.outd + 2 > blocksize ){
            std::string bname = blockname(filename,blockid);
            writefile(bname, buf, bufptr);
            std::cout << blockid << " " << cursize << std::endl;
            blockid++;
            cursize = 0;
            return false;
        }
        *((int*)bufptr) = v.outd;
        bufptr += sizeof(int);
        for( int i = 0; i < v.outd; i++ ){
            *((vid_t*)bufptr) = v.outv[i];
            bufptr += sizeof(vid_t);
        }
        cursize += v.outd + 2;
        return true;
    }

    struct cmp{
        bool operator()(int a, int b) {
            return a > b;
        }
    };

    void bfs( char * buf, char * &bufptr, vid_t u, char *bidx ){
        // std::queue<vid_t> Q;
        std::priority_queue< vid_t, std::vector<vid_t>, cmp > Q;
        Q.push( u );
        while( !Q.empty() ){
            // vid_t u = Q.front();
            vid_t u = Q.top();
            Q.pop();
            if(visited[u])  continue;
            visited[u] = true;
            int p = findInvl(u);
            if( p != curinvl ) { 
                loadInvl(p);
                curinvl = p;
                std::cout << u << " " << p << "  " << vertices.size() << " " << cursize << std::endl; 
            }
            Vertex v = vertices[u-invls[curinvl].first];
            if( !add_vertex(buf, bufptr, v) ) return ;
            *(bidx+v.vid) = (char)blockid;
            for( int i = 0; i < v.outd; i++ ){
                vid_t to = v.outv[i];
                if(!visited[to] ){
                    Q.push(to);
                }
            }
        }
    }

    vid_t minNotVisitedVertex( vid_t j ){
        for (int i = j; i < nvertices; ++i)
            if( !visited[i] )
                return i;
        return -1;
    }

    int partition( char **bidx ){
        //
        // blockid = find_partition(*bidx);
        // if( blockid > 0 )  return blockid;
        blockid = 0;

        SimPartition sim_partition_obj(filename);
        invls = sim_partition_obj.partition();
        invlnum = invls.size();
        nvertices = invls[invlnum-1].second + 1;
        visited = (bool*)malloc(nvertices*sizeof(bool));
        memset(visited, 0, nvertices);
        computeBlocksize();
        char * buf = (char*) malloc(blocksize*sizeof(int));
        char * bufptr = buf;
        mkdir((filename+"_block/").c_str(), 0777);
        curinvl = -1;
        cursize = 0;
        vid_t minnvv = 0;
        while( minnvv >= 0 ){
            bfs(buf, bufptr, minnvv, *bidx);
            minnvv = minNotVisitedVertex( minnvv );
        }
        return blockid;
    }
    
};

#endif