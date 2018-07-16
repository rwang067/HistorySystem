#ifndef DEF_GRAPHWALKER_WALK
#define DEF_GRAPHWALKER_WALK

#include <iostream>
#include <cstdio>
#include <sstream>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <string>
#include <queue>

#include "metrics/metrics.hpp"

typedef unsigned int WalkDataType;

class walkManager
{
public:
	int nshards, num_vertex;
	int nwalks,  lowerBound;
	std::string walk_filename;
	std::vector< std::queue<WalkDataType> > walks;
	std::vector< int > walkscnt;
	metrics &m;
public:
	walkManager( metrics &_m) : m(_m){}
	~walkManager(){
		walks.clear();
		walkscnt.clear();
	}

	void initialnizeWalks( int ns, int nv, std::string base_filename ){
		nshards = ns;

		num_vertex = nv;
		walks.resize(num_vertex);
		walkscnt.resize(num_vertex);

		walk_filename = base_filename + ".walks";
	     std::ofstream ofs;
	     ofs.open(walk_filename.c_str(), std::ofstream::out );
	     ofs << "random walks in  : " << base_filename << std::endl<< std::endl<< std::endl;
	     ofs.close();
	}

	void getWalkNum(int nw, int stopnw){
		nwalks  =  nw;
		lowerBound = stopnw;
	}

	WalkDataType encode( vid_t sourceId, int hop ){
		assert( hop < 512 );
		return (( sourceId & 0x7fffff ) << 9 ) | ( hop & 0x1ff ) ;
	}

	vid_t getSourceId( WalkDataType walk ){
		return ( walk >> 9 ) & 0x7fffff;
	}

	int getHop( WalkDataType walk ){
		return (walk & 0x1ff) ;
	}

	WalkDataType reencode( WalkDataType walk, vid_t toVertex ){
		int hop = getHop(walk) + 1;
		assert( hop < 512 );
		return (walk + 1);
	}

	WalkDataType getWalk( int v ){
		WalkDataType walk = walks[v].front();
		walks[v].pop();
		return walk;
	}

	void addWalk( int v, WalkDataType walk ){
		walks[v].push( walk );
	}

	int getWalkSize(int v){
		return walks[v].size();
	}

	int getWalksDis( int p ){
          	return walkscnt[p];
	}

	void moveWalk( WalkDataType walk, vid_t toVertex ){
		walk = reencode( walk, toVertex );
		walks[toVertex].push( walk );
	}

	bool emptyWalk( vid_t v ){
		return walks[v].empty();
	}

     bool notFinish(){
     		metrics_entry me = m.start_time();
     		int sum = 0;
          	for(int i=0; i < num_vertex; i++){
          		sum += walks[i].size();
          		if( sum > lowerBound ){
          			m.stop_time(me, "_check-finish");
          			return true;
          		}
          	}
          	m.stop_time(me, "_check-finish");
          	return false;
     }

     int intervalWithMaxWalks(){
     		metrics_entry me = m.start_time();
     		int maxw = 0, maxp = 0;
          	for(int p = 0; p < nshards; p++) {
	      	if( maxw < walkscnt[p] ){
          			maxw = walkscnt[p];
          			maxp = p;
          		}
	   	}
          	m.stop_time(me, "_find-block-with-max-walks");
          	return maxp;
     }

     void printWalksDistribution( int exec_block ){
     		metrics_entry me = m.start_time();
     		std::ofstream ofs;
	     ofs.open(walk_filename.c_str(), std::ofstream::out | std::ofstream::app );
	   	int sum = 0;
	  	for(int p = 0; p < nshards; p++) {
	      	sum += walkscnt[p];
	   	}
	  	ofs << exec_block << " \t " << walkscnt[exec_block] << " \t " << sum << std::endl;
	 	ofs.close();
	 	m.stop_time(me, "_print-walks-distribution");
     }
     
};

#endif