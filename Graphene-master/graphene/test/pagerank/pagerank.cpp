#include "cache_driver.h"
#include "IO_smart_iterator.h"
#include <stdlib.h>
#include <sched.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <asm/mman.h>
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
	//if(sa[vert_id]==criterion) 
		return true;
	//else 
	//	return false;
}

int main(int argc, char **argv) 
{
	std::cout<<"Format: /path/to/exe " 
		<<"#row_partitions #col_partitions thread_count "
		<<"/path/to/beg_pos_dir /path/to/csr_dir "
		<<"beg_header csr_header num_chunks "
		<<"chunk_sz (#bytes) concurr_IO_ctx "
		<<"max_continuous_useless_blk ring_vert_count num_buffs iteration\n";

	if(argc != 15)
	{
		fprintf(stdout, "Wrong input\n");
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
	index_t iteration = (index_t) atol(argv[14]);
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
  
	sa_t *rev_odeg_glb=(sa_t *)mmap(NULL,
		sizeof(sa_t)*vert_count,
		PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
		| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	
	if(rev_odeg_glb==MAP_FAILED)
	{	
		perror("mmap");
		exit(-1);
	}

	int *odeg_glb=(int *)mmap(NULL,
		sizeof(int)*vert_count,
		PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
		| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	
	if(odeg_glb==MAP_FAILED)
	{	
		perror("mmap");
		exit(-1);
	}

	//init rev_odeg and rank value
	sa_t init_rank=1.0/vert_count;
	for(index_t i=0;i<vert_count;i++)
	{
		sa_curr[i]=init_rank;
		sa_next[i] = 0;
		odeg_glb[i]=0;
		rev_odeg_glb[i]=0;
	}
  
	const index_t vert_per_blk = chunk_sz / sizeof(vertex_t);
	if(chunk_sz&(sizeof(vertex_t) - 1))
	{
		std::cout<<"Page size wrong\n";
		exit(-1);
	}

	char cmd[256];
	sprintf(cmd,"%s","iostat -x 1 -k > iostat_pagerank.log&");
	std::cout<<cmd<<"\n";
	//exit(-1);
	int *semaphore_acq = new int[1];
	int *semaphore_flag = new int[1];
	
	char *spin_lock = new char[vert_count];
	memset(spin_lock, 0, sizeof(char)*vert_count);
	semaphore_acq[0] = 0;
	semaphore_flag[0] = 0;
	//omp_lock_t gpu_semaphore;
	//omp_init_lock(&gpu_semaphore);
	//0 1 2 3 4 5 6 7 8 9 10 11 12 13 28 29 30 31 32 33 34 35 36 37 38 39 40 41
	//14 15 16 17 18 19 20 21 22 23 24 25 26 27 42 43 44 45 46 47 48 49 50 51 52 53 54 55
	//int core_id[8]={0, 2, 4, 6, 14, 16, 18, 20};
	//int core_id[16]={0, 2, 4, 6, 8, 10, 12, 28, 14, 16, 18, 20, 22, 24, 26, 42};
	//int core_id[16]={0, 2, 4, 12, 14, 16, 1, 3, 6, 8, 10, 18, 20, 22, 7, 9};
	//int core_id[16]={0, 2, 4, 6, 8, 10, 12, 28, 30, 32, 34, 36, 38, 40, 1, 3};
	//int socket_one[12]={0, 6, 1, 7, 2, 8, 3, 9, 4, 10, 5, 11};
	//int socket_two[12]={12, 18, 13, 19, 14, 20, 15, 21, 16, 22, 17, 23};
	int socket_one[12]={0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 16, 17};
	int socket_two[12]={6, 7, 8, 9, 10, 11, 18, 19, 20, 21, 22, 23};

	sa_t *sa_dummy;
	IO_smart_iterator **it_comm = new IO_smart_iterator*[NUM_THDS];
	
	double tm = 0;
#pragma omp parallel \
	num_threads (NUM_THDS) \
	shared(sa_next,sa_curr,rev_odeg_glb,comm)
	{
		std::stringstream travss;
		travss.str("");
		travss.clear();
		
		std::stringstream fetchss;
		fetchss.str("");
		fetchss.clear();

		std::stringstream savess;
		savess.str("");
		savess.clear();
		
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
		//pin_thread(core_id,tid);
		if(tid < 16) 
			pin_thread_socket(socket_one, 12);
		else
			pin_thread_socket(socket_two, 12);
		
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
		} // End of if((tid&1) == 0) 
#pragma omp barrier
		IO_smart_iterator *it = it_comm[(tid>>1)<<1];
		
		if(!tid) system((const char *)cmd);
#pragma omp barrier
		if(tid==0) std::cout<<"Aggregate degree ... \n";	
			
		//Degree is ensured to be correct
		if((tid & 1 ) == 0)
			for(vertex_t vert=it->row_ranger_beg;
					vert<it->row_ranger_end;vert++)
			{
				int my_degree = (int)(it->beg_pos_ptr[vert+1-it->row_ranger_beg]-
						it->beg_pos_ptr[vert-it->row_ranger_beg]);

				__sync_fetch_and_add(odeg_glb+vert,my_degree);
			}
	

#pragma omp barrier
		if(tid==0) std::cout<<"Reverse degree ... \n";	
		for(vertex_t vert=beg_1d;vert<end_1d;vert++)
		{
			if(odeg_glb[vert])
			{
				rev_odeg_glb[vert]=1.0/odeg_glb[vert];
				it->sa_ptr[vert] *= rev_odeg_glb[vert];
			}
			else rev_odeg_glb[vert]=0;
		}
		
#pragma omp barrier
		if(tid==0) std::cout<<"Start pagerank computation ... \n";	
		while(true)
		{
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
#pragma omp barrier

			if((tid & 1) == 0)
			{
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

					if(chunk_id == -1) break;
					struct chunk *pinst = it->cd->cache[chunk_id];	
					index_t blk_beg_off = pinst->blk_beg_off;
					index_t num_verts = pinst->load_sz;
					vertex_t vert_id = pinst->beg_vert;

					//process one chunk
					while(true)
					{
						index_t beg = beg_pos[vert_id - it->row_ranger_beg] 
							- blk_beg_off;
						index_t end = beg + beg_pos[vert_id + 1 - 
							it->row_ranger_beg]- 
							beg_pos[vert_id - it->row_ranger_beg];

						//possibly vert_id starts from preceding data block.
						//there by beg<0 is possible
						if(beg<0) beg = 0;

						if(end>num_verts) end = num_verts;
						for( ;beg<end; ++beg)
						{
							vertex_t nebr = pinst->buff[beg];
							sa_next[vert_id] += sa_curr[nebr];
						}
						++vert_id;

						if(vert_id >= it->row_ranger_end) break;
						if(beg_pos[vert_id - it->row_ranger_beg]
								- blk_beg_off > num_verts) 
							break;
					}

					pinst->status = EVICTED;
					assert(it->cd->circ_free_chunk->en_circle(chunk_id)!= -1);

				}
				
				//work-steal
				//for(int ii = 0; ii < NUM_THDS ; ii += 2)
			//	for(int ii = tid - (col_par * 2); ii <= tid + (col_par * 2) ; ii += (col_par *4))
			//	{
			//		if(ii < 0 || ii >= NUM_THDS) continue;
			//		//if(ii == tid || ii >= NUM_THDS || ii < 0) 
			//		//if(ii == tid) 
			//		//	continue;
			//		
			//		IO_smart_iterator* it_work_steal = it_comm[ii];			

			//		//if(it_work_steal->my_col != it->my_col) continue;

			//		while(true)
			//		{	
			//			int chunk_id = -1;
			//			double blk_tm = wtime();
			//			while((chunk_id = it_work_steal->cd->circ_load_chunk->de_circle())
			//					== -1)
			//			{
			//				if(it_work_steal->is_bsp_done)
			//				{
			//					chunk_id = it_work_steal->cd->circ_load_chunk->de_circle();
			//					break;
			//				}
			//			}
			//			it_work_steal->wait_io_time += (wtime() - blk_tm);

			//			if(chunk_id == -1) break;
			//			
			//			//printf("%dhelps%d-for%d\n", tid, ii, chunk_id);
			//			struct chunk *pinst = it_work_steal->cd->cache[chunk_id];	
			//			index_t blk_beg_off = pinst->blk_beg_off;
			//			index_t num_verts = pinst->load_sz;
			//			vertex_t vert_id = pinst->beg_vert;

			//			//process one chunk
			//			while(true)
			//			{
			//				index_t beg = it_work_steal->beg_pos_ptr[vert_id - it_work_steal->row_ranger_beg] 
			//					- blk_beg_off;
			//				index_t end = beg + it_work_steal->beg_pos_ptr[vert_id + 1 - 
			//					it_work_steal->row_ranger_beg]- 
			//					it_work_steal->beg_pos_ptr[vert_id - it_work_steal->row_ranger_beg];

			//				//possibly vert_id starts from preceding data block.
			//				//there by beg<0 is possible
			//				if(beg<0) beg = 0;

			//				if(end>num_verts) end = num_verts;
			//				for( ;beg<end; ++beg)
			//				{
			//					vertex_t nebr = pinst->buff[beg];
			//					sa_next[vert_id] += sa_curr[nebr];
			//				}
			//				++vert_id;

			//				if(vert_id >= it_work_steal->row_ranger_end) break;
			//				if(it_work_steal->beg_pos_ptr[vert_id - it_work_steal->row_ranger_beg]
			//						- blk_beg_off > num_verts) 
			//					break;
			//			}


			//			pinst->status = EVICTED;
			//			assert(it_work_steal->cd->circ_free_chunk->en_circle(chunk_id)!= -1);
			//		}
			//	}

				it->front_count[comp_tid] = front_count;
			}
			else
			{
				while(it->is_bsp_done == false)
				{
					it->load_kv_vert_full(level);
					//it->load_key_iolist(level);
				}
			}

finish_point:
			++level;
#pragma omp barrier
			for(vertex_t vert = beg_1d;vert < end_1d; vert ++)
			{
				sa_curr[vert] = (sa_next[vert] * 0.85 + 0.15) * rev_odeg_glb[vert];
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
				
			if (!tid && level==iteration) printLog(level, vert_count, sa_curr, beg_dir, true);
			if(level == iteration) break;
//			if(tid ==0) printf("%f %f %f %f %f %f %f %f %f\n", it->sa_ptr[121]*odeg_glb[121], it->sa_ptr[27]*odeg_glb[27], it->sa_ptr[52]*odeg_glb[52], it->sa_ptr[49]*odeg_glb[49], it->sa_ptr[95]*odeg_glb[95], it->sa_ptr[1884]*odeg_glb[1884], it->sa_ptr[2]*odeg_glb[2], it->sa_ptr[12]*odeg_glb[12], it->sa_ptr[131]*odeg_glb[131]);

		} // End of while

		if(!tid)system("killall iostat");

		if(!tid) std::cout<<"Total time: "<<tm<<" second(s)\n";

		if((tid & 1) == 0) delete it;
		
	} // End of share
	munmap(sa_next,sizeof(sa_t)*vert_count);
	munmap(sa_curr,sizeof(sa_t)*vert_count);
	munmap(odeg_glb,sizeof(index_t)*vert_count);
	munmap(rev_odeg_glb,sizeof(sa_t)*vert_count);
	delete[] comm;
	return 0;
}
