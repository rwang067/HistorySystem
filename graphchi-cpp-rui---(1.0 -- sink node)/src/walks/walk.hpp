#ifndef DEF_GRAPHCHI_WALK
#define DEF_GRAPHCHI_WALK


#include <iostream>
#include <cstdio>
#include <sstream>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <string>

#include "api/graph_objects.hpp"
#include "metrics/metrics.hpp"
#include "io/stripedio.hpp"
#include "graphchi_types.hpp"


namespace graphchi {

	typedef unsigned int WalkDataType;

	class walkManager
	{
	protected:
		int nshards, num_vertex;
		std::vector< std::vector <WalkDataType> > walks;
		std::vector<std::pair<vid_t, vid_t> > intervals;
		std::string walk_filename;
	public:
		walkManager(){}
		~walkManager(){
			walks.clear();
		}

		void initialnizeWalks( int ns, int nv, std::string base_filename, std::vector<std::pair<vid_t, vid_t> > in ){
			nshards = ns;
			num_vertex = nv;
			walk_filename = base_filename + ".walks";
			intervals = in;
			walks.resize(num_vertex);
		     std::ofstream ofs;
		     ofs.open(walk_filename.c_str(), std::ofstream::out );
		     ofs << "random walks in  : " << base_filename << std::endl<< std::endl<< std::endl;
		     ofs.close();
		}

		WalkDataType encode( vid_t sourceId, int hop ){
			assert( hop < 1024 );
			return (( sourceId & 0x3fffff ) << 10 ) | ( hop & 0x3ff ) ;
		}

		vid_t getSourceId( WalkDataType walk ){
			return (( walk & 0xfffffc0 ) >> 10 ) & 0x3fffff;
		}

		int getHop( WalkDataType walk ){
			return (walk & 0x3ff) ;
		}

		WalkDataType reencode( WalkDataType walk, vid_t toVertex ){
			int hop = getHop(walk) + 1;
			assert( hop < 1024 );
			return (walk + 1);
		}

		WalkDataType getWalk( int v, int i ){
			return walks[v][i];
		}

		void addWalk( int v, WalkDataType walk ){
			walks[v].push_back( walk );
		}

		int getWalkSize(int v){
			return walks[v].size();
		}

		void clearWalk( int v ){
			walks[v].clear();
		}

		int getWalksDis( int p ){
			int walknum = 0;
		  	for( unsigned int i = intervals[p].first; i <= intervals[p].second; i++ )
		  		walknum += walks[i].size();
			return walknum;
		}

		void moveWalk( WalkDataType walk, vid_t toVertex ){
			walk = reencode( walk, toVertex );
			walks[toVertex].push_back( walk );
		}

	     bool notEmpty(){
	     		int sum = 0;
	          	for(int i=0; i < num_vertex; i++){
	          		sum += walks[i].size();
	          		if( sum > 0 )
	          		// if( sum > 480000 )
	          			return true;
	          	}
	          	return false;
	     }

	     int intervalWithMaxWalks(){
	     		int maxw = 0, maxp = 0;
	          	for(int p = 0; p < nshards; p++) {
		  		int walknum = 0;
		  		for( unsigned int i = intervals[p].first; i <= intervals[p].second; i++ )
		  			walknum += walks[i].size();
		      	if( maxw < walknum ){
	          			maxw = walknum;
	          			maxp = p;
	          		}
		   	}
	          	return maxp;
	     }

	     void printWalksDistribution( int exec_interval ){
	     		std::ofstream ofs;
		     ofs.open(walk_filename.c_str(), std::ofstream::out | std::ofstream::app );
		  	// ofs << "walksvector after exec_interval:  " << exec_interval << std::endl;
		   	int sum = 0;
		  	for(int p = 0; p < nshards; p++) {
		  		int walknum = 0;
		  		for( unsigned int i = intervals[p].first; i <= intervals[p].second; i++ )
		  			walknum += walks[i].size();
		      	// ofs << p  << " " << walknum << std::endl;
		      	sum += walknum;
		   	}
		  	// ofs << " Sum : " << sum << std::endl;
		  	ofs << exec_interval << " \t " << sum << std::endl;

		 	ofs.close();
	     }

	};
}

#endif