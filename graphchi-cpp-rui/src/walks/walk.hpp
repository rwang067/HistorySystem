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
		int nwalks,  lowerBound, intervalLowerBound;
		std::string walk_filename;
		std::vector< std::queue <WalkDataType> > walks;
		std::vector< std::pair<vid_t, vid_t> > intervals;
		metrics &m;
	public:
		std::vector<int> minstep;
		walkManager( metrics &_m) : m(_m){}
		~walkManager(){
			walks.clear();
			intervals.clear();
		}

		void initialnizeWalks( int ns, int nv, std::string base_filename, std::vector<std::pair<vid_t, vid_t> > in ){
			nshards = ns;

			num_vertex = nv;
			walks.resize(num_vertex);
			
			walk_filename = base_filename + ".walks";
		     std::ofstream ofs;
		     ofs.open(walk_filename.c_str(), std::ofstream::out );
		     ofs << "random walks in  : " << base_filename << std::endl<< std::endl<< std::endl;
		     ofs.close();

			intervals = in;

			minstep.resize(nshards);
			for( int i = 0; i < nshards; i++ )
				minstep[i] = 0;
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
			int walknum = 0;
		  	for( unsigned int i = intervals[p].first; i <= intervals[p].second; i++ ){
		  		walknum += walks[i].size();
	          	}
	          	return walknum;
		}

		void moveWalktoHop( WalkDataType walk, vid_t toVertex, int hop ){
			walk = encode(getSourceId(walk), hop);
			walks[toVertex].push( walk );

			int curp = findInvl(toVertex);
			if( getHop(walk) < minstep[curp] )
				minstep[curp] = getHop(walk);
		}

		void moveWalk( WalkDataType walk, vid_t toVertex ){
			walk = reencode( walk, toVertex );
			walks[toVertex].push( walk );

			int curp = findInvl(toVertex);
			if( getHop(walk) < minstep[curp] )
				minstep[curp] = getHop(walk);

			//print the vertices trace of a long walk
			/*std::ofstream ofs;
			ofs.open(walk_filename.c_str(), std::ofstream::out | std::ofstream::app );
		      ofs << " -> " << toVertex;
		      ofs.close();*/

			//print the interval trace of a long walk
	          	/*std::ofstream ofs;
			ofs.open(walk_filename.c_str(), std::ofstream::out | std::ofstream::app );
			ofs << curp << std::endl;
		      ofs.close();*/	
		}

		int findInvl( vid_t v ){
			for( int p = 0; p < nshards; p++ )
		  		if( v <= intervals[p].second )
		  			return p;
	          	return nshards;
		}

		bool emptyWalk( unsigned int v ){
			return walks[v].empty();
		}

		bool notFinishInterval( int p ){
			metrics_entry me = m.start_time();
			int walknum = 0;
		  	for( unsigned int i = intervals[p].first; i <= intervals[p].second; i++ ){
		  		walknum += walks[i].size();
	          		// std::cout << "notFinishInterval interval : " << i << " " << walknum << " " << 0 << std::endl;
	          	}
	          	m.stop_time(me, "_check-interval-finish");
	          	// std::cout << "notFinishInterval (walknum, walknum>0? ): " << walknum << " " << (walknum > 0) << std::endl;
	          	return walknum > 0;
		}

	     bool notFinish(){
	     		metrics_entry me = m.start_time();
	     		int sum = 0;
	          	for(int i=0; i < num_vertex; i++){
	          		sum += walks[i].size();
	          		if( sum > lowerBound ){
	          			m.stop_time(me, "_check-finish");
	          			// std::cout << "notFinish ( i, sum ) : " << i << " , " << sum << std::endl;
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
		  		int walknum = 0;
		  		for( unsigned int i = intervals[p].first; i <= intervals[p].second; i++ )
		  			walknum += walks[i].size();
		      	if( maxw < walknum ){
	          			maxw = walknum;
	          			maxp = p;
	          		}
		   	}
	          	m.stop_time(me, "_find-interval-with-max-walks");
	          	return maxp;
	     }

	     int intervalWithMinStep(){
	     		metrics_entry me = m.start_time();
	     		int mins = 0xfffffff, minp = 0;
	          	for(int p = 0; p < nshards; p++) {
			      	if( mins > minstep[p] ){
	          			mins = minstep[p];
	          			minp = p;
	          		}
		   	}
	          	m.stop_time(me, "_find-interval-with-min-steps");
	          	return minp;
	     }

	     int intervalWithMaxWeight(){
	     		metrics_entry me = m.start_time();
	     		float maxwt = 0;
	     		int maxp = 0;
	          	for(int p = 0; p < nshards; p++) {
		  		int walknum = 0;
		  		for( unsigned int i = intervals[p].first; i <= intervals[p].second; i++ )
		  			walknum += walks[i].size();
		      		if(  maxwt < (float)walknum/minstep[p] ){
	          			maxwt = (float)walknum/minstep[p];
	          			maxp = p;
	          		}
		   	}
	          	m.stop_time(me, "_find-interval-with-max-weight");
	          	return maxp;
	     }

	     int intervalWithRandom(){
	     		metrics_entry me = m.start_time();
	     		int ranp = rand() % nshards;
	          	m.stop_time(me, "_find-interval-with-random");
	          	return ranp;
	     }

	     void printWalksDistribution( int exec_interval ){
	     		metrics_entry me = m.start_time();
	     		std::ofstream ofs;
		     ofs.open(walk_filename.c_str(), std::ofstream::out | std::ofstream::app );
		  	// ofs << "walksvector after exec_interval:  " << exec_interval << std::endl;
		   	int sum = 0;
		   	int *walknum = new int[nshards];
		  	for(int p = 0; p < nshards; p++) {
		  		walknum[p] = 0;
		  		for( unsigned int i = intervals[p].first; i <= intervals[p].second; i++ )
		  			walknum[p] += walks[i].size();
		      	sum += walknum[p];
		      	//ofs << p  << ":( " << walknum[p] << " ) \t ";
		   	}
		  	// ofs << " Sum : " << sum << std::endl;
		  	ofs << exec_interval << " \t " << walknum[exec_interval] << " \t " << sum << std::endl;
		  	delete walknum;
		 	ofs.close();
		 	m.stop_time(me, "_print-walks-distribution");
	     }

	};
}

#endif