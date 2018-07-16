#include "cache_driver.h"
#include "IO_smart_iterator.h"
#include <stdlib.h>
#include <sched.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <asm/mman.h>
#include<time.h>
#include "pin_thread.h"
#include <algorithm>
#include "get_vert_count.hpp"
#include "get_col_ranger.hpp"
#include "outputLog.hpp"

inline bool is_active
(index_t vert_id,
sa_t criterion,
sa_t *sa, sa_t *sa_prev)
{
	if(sa[vert_id]) 
		return true;
	// std::cout<<"vert_id: " << vert_id << std::endl;
	return false;
}

int main(int argc, char **argv) 
{
	std::cout<<"Format: /path/to/exe " 
		<<"#row_partitions #col_partitions thread_count "
		<<"/path/to/beg_pos_dir /path/to/csr_dir "
		<<"beg_header csr_header num_chunks "
		<<"chunk_sz (#bytes) concurr_IO_ctx "
		<<"max_continuous_useless_blk ring_vert_count num_buffs num_walks num_steps\n";

	if(argc != 16)
	{
		fprintf(stdout, "Wrong input, argc = %d != 16\n", argc);
		exit(-1);
	}

	//Output input
	for(int i=0;i<argc;i++)
		std::cout<<argv[i]<<" ";
	std::cout<<"\n";

	const int row_par = atoi(argv[1]);
	const int col_par = atoi(argv[2]);
	const int NUM_THDS = atoi(argv[3]);
	const char *beg_dir = argv[4];
	const char *csr_dir = argv[5];
	const char *beg_header=argv[6];
	const char *csr_header=argv[7];
	const index_t num_chunks = atoi(argv[8]);
	const size_t chunk_sz = atoi(argv[9]);
	const index_t io_limit = atoi(argv[10]);
	const index_t MAX_USELESS = atoi(argv[11]);
	const index_t ring_vert_count = atoi(argv[12]);
	const index_t num_buffs = atoi(argv[13]);
	index_t num_walks = (index_t) atol(argv[14]);
	index_t num_steps = (index_t) atol(argv[15]);
	assert(NUM_THDS==(row_par*col_par*2));
	
	sa_t *sa_curr=NULL;
	sa_t *sa_next=NULL;
	vertex_t **front_queue_ptr;
	index_t *front_count_ptr;
	vertex_t *col_ranger_ptr;
	index_t *comm = new index_t[NUM_THDS];
	
	const index_t vert_count=get_vert_count
		(comm, beg_dir,beg_header,row_par,col_par);
	get_col_ranger(col_ranger_ptr, front_queue_ptr,
			front_count_ptr, beg_dir, beg_header,
			row_par, col_par);
	
	sa_curr=(sa_t *)mmap(NULL,sizeof(sa_t)*vert_count,
		PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
		| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	if(sa_curr==MAP_FAILED)
	{	
		perror("mmap");
		exit(-1);
	}
	
	sa_next=(sa_t *)mmap(NULL,sizeof(sa_t)*vert_count,
		PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
		| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	
	if(sa_next==MAP_FAILED)
	{	
		perror("mmap");
		exit(-1);
	}

	//init rev_odeg and rank value
	for(index_t i=0;i<vert_count;i++)
	{
		sa_curr[i]= num_walks;
		sa_next[i] = 0;
	}
  
	const index_t vert_per_chunk = chunk_sz / sizeof(vertex_t);
	if(chunk_sz&(sizeof(vertex_t) - 1))
	{
		std::cout<<"Page size wrong\n";
		exit(-1);
	}

	char cmd[256];
	sprintf(cmd,"%s","iostat -x 1 -k > iostat_randomwalks.log&");
	std::cout<<cmd<<"\n";
	//exit(-1);

	sa_t *sa_dummy;
	IO_smart_iterator **it_comm = new IO_smart_iterator*[NUM_THDS];
	
	srand((unsigned)time(NULL));
	double tm = 0;
#pragma omp parallel \
	num_threads (NUM_THDS) \
	shared(sa_next,sa_curr,comm)
	{
		sa_t level = 0;
		int tid = omp_get_thread_num();
		int comp_tid = tid >> 1;
		comp_t *neighbors;
		vertex_t *sources;
		vertex_t dest, source;
		index_t *beg_pos=NULL;
		
		//use all threads in 1D partition manner.
		index_t step_1d = vert_count / NUM_THDS;
		index_t beg_1d = tid * step_1d;
		index_t end_1d = beg_1d + step_1d;
		if(tid==NUM_THDS-1) end_1d = vert_count;
		
		if((tid&1) == 0) 
		{
			IO_smart_iterator *it_temp = 
				new IO_smart_iterator(
						true,
						front_queue_ptr,
						front_count_ptr,
						col_ranger_ptr,
						comp_tid,comm,
						row_par,col_par,									
						beg_dir,csr_dir, 
						beg_header,csr_header,
						num_chunks,
						chunk_sz,
						sa_curr,sa_dummy,beg_pos,
						num_buffs,
						ring_vert_count,
						MAX_USELESS,
						io_limit,
						&is_active);

			it_comm[tid] = it_temp;
			it_comm[tid]->is_bsp_done = false;
		}
#pragma omp barrier
		IO_smart_iterator *it = it_comm[(tid>>1)<<1];
		
		if(!tid) system((const char *)cmd);
#pragma omp barrier
		if(tid==0) std::cout<<"Start random walks ... \n";	
		while(true)
		{
			std::cout<<"Start running level " << level << std::endl;
			//- Framework gives user block to process
			//- Figures out what blocks needed next level
			if((tid & 1) == 0)
			{
				it -> io_time = 0;
				it -> wait_io_time = 0;
				it -> wait_comp_time = 0;
				it -> cd -> io_submit_time = 0;
				it -> cd -> io_poll_time = 0;
				it -> cd -> fetch_sz = 0;

				//not early terminate
				it -> reqt_blk_count = 1;//
				//as long as not 0
			}

			index_t front_count = 0;
			double convert_tm = 0;	
			double ltm=wtime();

			if ((tid&1)==0)
			{
				it->req_translator(0);
				it->is_bsp_done = false;
				convert_tm = wtime() - convert_tm;
			}
			else
			{
				it->is_io_done = false;
			}
#pragma omp barrier

			if((tid & 1) == 0)
			{ int s, e ; s = e = 0;
				while(true)
				{	
					int chunk_id = -1;
					double blk_tm = wtime();
					while((chunk_id = it->cd->circ_load_chunk->de_circle())
							== -1)
					{
						if(it->is_bsp_done)
						{
							chunk_id = it->cd->circ_load_chunk->de_circle();
							break;
						}
					}
					it->wait_io_time += (wtime() - blk_tm);
					// std::cout  << "chunk_id=" << chunk_id << "\t load_chunk_sz " << it->cd->circ_load_chunk->get_sz() <<"\t free_chunk_sz :" << it->cd->circ_free_chunk->get_sz() << std::endl;

					if(chunk_id == -1) break;
					struct chunk *pinst = it->cd->cache[chunk_id];	
					index_t blk_beg_off = pinst->blk_beg_off;
					index_t num_verts = pinst->load_sz;
					vertex_t vert_id = pinst->beg_vert;

					// std::cout  << "chunk_id=" << chunk_id << "\t" << "beg_vert=" << pinst->beg_vert << "\t" << "blk_beg_off =" << blk_beg_off << "\t" << "                     s =" << s++ <<  std::endl;

					//process one chunk
					while(true)
					{
						if( sa_curr[vert_id] > 0 ) //if there are some walks in the vertex
						{
							// get a vertex with beg and end in buff --> vertex_id
							index_t beg = beg_pos[vert_id - it->row_ranger_beg] - blk_beg_off;
							index_t end = beg_pos[vert_id + 1 - it->row_ranger_beg] - blk_beg_off;

							//possibly vert_id starts from preceding data block.
							//there by beg<0 is possible
							if(beg<0) beg = 0;
							if(end>num_verts) end = num_verts;

							index_t num_walks = (index_t) sa_curr[vert_id];

			                    	for (int i = 0; i < num_walks; i++){
				                          vertex_t dstId;
				                          //if there is out-neighbors , with 0.85 random select one
				                          if (((float)rand())/RAND_MAX > 0.15 && (end>beg)){
				                           	dstId = pinst->buff[rand() % (end-beg) + beg];
				                          }else{
				                                dstId = rand() % vert_count;
				                          }
				                          sa_next[dstId]++;
			                    	}
						}
						vert_id++;

						if(vert_id >= it->row_ranger_end){
							// std::cout  << "chunk_id= " << chunk_id << "\t" << pinst->beg_vert << "\t" << vert_id-1 << "\t" << e++ <<  std::endl;
							break;}
						if(beg_pos[vert_id - it->row_ranger_beg] - blk_beg_off > num_verts) { 
							// std::cout  << "chunk_id=" << chunk_id << "\t" << pinst->beg_vert << "\t" <<  vert_id-1 << "\t" << e++ <<  std::endl;
							break;}
					}

					pinst->status = EVICTED;
					assert(it->cd->circ_free_chunk->en_circle(chunk_id)!= -1);
					// std::cout << "free_chunk : \tXX " << chunk_id << std::endl;

				}
				it->front_count[comp_tid] = front_count;
			}
			else
			{
				while(it->is_bsp_done == false)
				{
					it->load_key(level);
					// it->load_kv_vert_full(level);
					//it->load_key_iolist(level);
				}
			}

finish_point:
			++level;
#pragma omp barrier
			for(vertex_t vert = beg_1d;vert < end_1d; vert ++)
			{
				sa_curr[vert] = sa_next[vert];
				sa_next[vert] = 0;
			}
#pragma omp barrier
			ltm = wtime() - ltm;

			if(tid == 0) tm += ltm;
#pragma omp barrier
			comm[tid] = it->cd->fetch_sz;
#pragma omp barrier
			index_t total_sz = 0;
			for(int i = 0 ;i< NUM_THDS; ++i)
				total_sz += comm[i];
			total_sz >>= 1;//total size doubled
			
			if(!tid) std::cout<<"@level-"<<(int)level
				<<"-font-leveltime-converttm-iotm-waitiotm-waitcomptm-iosize: "
				<<front_count<<" "<<ltm<<" "<<convert_tm<<" "<<it->io_time
				<<"("<<it->cd->io_submit_time<<","<<it->cd->io_poll_time<<") "
				<<" "<<it->wait_io_time<<" "<<it->wait_comp_time<<" "
				<<total_sz<<"\n";

			if (!tid && level==num_steps) printLog(level, vert_count, sa_curr, beg_dir, true);
			if(level == num_steps) break;
//			if(tid ==0) printf("%f %f %f %f %f %f %f %f %f\n", it->sa_ptr[121]*odeg_glb[121], it->sa_ptr[27]*odeg_glb[27], it->sa_ptr[52]*odeg_glb[52], it->sa_ptr[49]*odeg_glb[49], it->sa_ptr[95]*odeg_glb[95], it->sa_ptr[1884]*odeg_glb[1884], it->sa_ptr[2]*odeg_glb[2], it->sa_ptr[12]*odeg_glb[12], it->sa_ptr[131]*odeg_glb[131]);

		}

		if(!tid)system("killall iostat");

		if(!tid) std::cout<<"Total time: "<<tm<<" second(s)\n";

		if((tid & 1) == 0) delete it;
		
	}
	munmap(sa_next,sizeof(sa_t)*vert_count);
	munmap(sa_curr,sizeof(sa_t)*vert_count);
	delete[] comm;
	return 0;
}
