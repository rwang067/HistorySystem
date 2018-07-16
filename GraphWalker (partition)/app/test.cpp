#include <iostream>
#include <string>

#include "preprocess/bfspartition.hpp"

int main(int argc, char const *argv[])
{
    std::string filename = "../dataset/C++-LiveJournal1/soc-LiveJournal1.txt";
    int nvertices = 4847571;
    char *bidx = (char*) malloc(nvertices*sizeof(char));
    BfsPartition bfs_partition_obj(filename);
    int nblocks = bfs_partition_obj.partition(&bidx);
    std::cout << nblocks << std::endl;
    free(bidx);
    return 0;
}