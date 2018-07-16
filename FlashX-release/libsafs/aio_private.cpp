/*
 * Copyright 2014 Open Connectome Project (http://openconnecto.me)
 * Written by Da Zheng (zhengda1936@gmail.com)
 *
 * This file is part of SAFSlib.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <limits.h>

#include <boost/assert.hpp>

#include "aio_private.h"
#include "messaging.h"
#include "read_private.h"
#include "file_partition.h"
#include "slab_allocator.h"

template class blocking_FIFO_queue<safs::thread_callback_s *>;

namespace safs
{

#define EVEN_DISTRIBUTE

const int MAX_EMBED_BUFS = 64;

/* 
 * each file gets the same number of outstanding requests.
 */
#ifdef EVEN_DISTRIBUTE
#define MAX_OUTSTANDING_NREQS (AIO_DEPTH / num_open_files())
#define ALLOW_DROP
#endif

struct thread_callback_s
{
	struct io_callback_s cb;
	async_io *aio;
	callback_allocator *cb_allocator;
	io_request req;
	embedded_array<struct iovec, MAX_EMBED_BUFS> vec;
};

/**
 * This slab allocator makes sure all requests in the callback structure
 * are extended requests.
 */
class callback_allocator: public obj_allocator<thread_callback_s>
{
	class callback_initiator: public obj_initiator<thread_callback_s>
	{
	public:
		void init(thread_callback_s *cb) {
			new (&cb->req) io_request();
		}
	};
public:
	callback_allocator(int node_id, long increase_size,
			long max_size = params.get_max_obj_alloc_size()): obj_allocator<thread_callback_s>(
				std::string("aio_cb_allocator-") + itoa(node_id), node_id,
				true, increase_size, max_size,
				obj_initiator<thread_callback_s>::ptr(new callback_initiator())) {
	}
};

void aio_callback(io_context_t ctx, struct iocb* iocb[],
		void *cbs[], long res[], long res2[], int num) {
	async_io *aio = NULL;
	thread_callback_s *tcbs[num];
	for (int i = 0; i < num; i++) {
		assert(res2[i] == 0);
		tcbs[i] = (thread_callback_s *) cbs[i];
		if (aio == NULL)
			aio = tcbs[i]->aio;
		// This is true when disks are only accessed by disk access threads.
		assert(aio == tcbs[i]->aio);
	}

	aio->return_cb(tcbs, num);
}

async_io::io_ref::io_ref(buffered_io *io)
{
	this->io = std::shared_ptr<buffered_io>(io);
	this->count = 1;
}

void async_io::io_ref::dec_ref()
{
	count--;
	if (count == 0) {
		io->cleanup();
		io.reset();
	}
}

async_io::async_io(const logical_file_partition &partition,
		int aio_depth_per_file, thread *t, const safs_header &header,
		int flags): io_interface(t, header), AIO_DEPTH(
			aio_depth_per_file * partition.get_num_files())
{
	int node_id = t->get_node_id();
	cb_allocator = new callback_allocator(node_id,
			AIO_DEPTH * sizeof(thread_callback_s));;
	buf_idx = 0;
	ctx = new aio_ctx_impl(node_id, AIO_DEPTH);

	num_iowait = 0;
	num_completed_reqs = 0;
	open_flags = flags;
	if (partition.is_active()) {
		int file_id = partition.get_file_id();
		io_ref io(new buffered_io(partition, t, header, O_DIRECT | flags));
		default_io = io;
		open_files.insert(std::pair<int, io_ref>(file_id, io));
	}
}

void async_io::cleanup()
{
	int slot = ctx->max_io_slot();

	while (slot < AIO_DEPTH) {
		ctx->io_wait(NULL, 1);
		slot = ctx->max_io_slot();
	}
	for (auto it = open_files.begin(); it != open_files.end(); it++) {
		// Files may have been closed.
		if (it->second.is_valid())
			it->second.get_io().cleanup();
	}
}

async_io::~async_io()
{
	cleanup();
	delete ctx;
	open_files.clear();
	delete cb_allocator;
}

int async_io::get_file_id() const
{
	if (default_io.is_valid())
		return default_io.get_io().get_file_id();
	else
		return -1;
}

struct iocb *async_io::construct_req(io_request &io_req, callback_t cb_func)
{
	thread_callback_s *tcb = cb_allocator->alloc_obj();
	io_callback_s *cb = (io_callback_s *) tcb;

	cb->func = cb_func;
	// init doesn't pass the ownership of an extension from one request
	// to another.
	tcb->req = io_req;
	tcb->aio = this;
	tcb->cb_allocator = cb_allocator;

	assert(tcb->req.get_size() >= MIN_BLOCK_SIZE);
	assert(tcb->req.get_size() % MIN_BLOCK_SIZE == 0);
	assert(tcb->req.get_offset() % MIN_BLOCK_SIZE == 0);
	assert((long) tcb->req.get_buf() % MIN_BLOCK_SIZE == 0);
	int io_type = tcb->req.get_access_method() == READ ? A_READ : A_WRITE;
	block_identifier bid;
	auto it = open_files.find(io_req.get_file_id());
	assert(it != open_files.end());
	assert(it->second.is_valid());
	buffered_io &io = it->second.get_io();
	io.get_partition().map(tcb->req.get_offset() / PAGE_SIZE, bid);
	// Here we translate the global request offset to the offset in the local
	// disk.
	off_t local_off = bid.off * PAGE_SIZE + (tcb->req.get_offset() % PAGE_SIZE);
	if (tcb->req.get_num_bufs() == 1)
		return ctx->make_io_request(io.get_fd(tcb->req.get_offset()),
				tcb->req.get_size(), local_off, tcb->req.get_buf(), io_type, cb);
	else {
		int num_bufs = tcb->req.get_num_bufs();
		for (int i = 0; i < num_bufs; i++) {
			assert((long) tcb->req.get_buf(i) % MIN_BLOCK_SIZE == 0);
			assert(tcb->req.get_buf_size(i) % MIN_BLOCK_SIZE == 0);
		}
		tcb->vec.resize(num_bufs);
		BOOST_VERIFY(tcb->req.get_vec(tcb->vec.data(), num_bufs) == num_bufs);
		struct iocb *req = ctx->make_iovec_request(io.get_fd(tcb->req.get_offset()),
				/* 
				 * iocb only contains a pointer to the io vector.
				 * the space for the IO vector is stored
				 * in the callback structure.
				 */
				tcb->vec.data(), num_bufs, local_off, io_type, cb);
		// I need to submit the request immediately. The iovec array is
		// allocated in the stack.
		ctx->submit_io_request(&req, 1);
		return NULL;
	}
}

void async_io::access(io_request *requests, int num, io_status *status)
{
	ASSERT_EQ(get_thread(), thread::get_curr_thread());
	while (num > 0) {
		int slot = ctx->max_io_slot();
		if (slot == 0) {
			/*
			 * To achieve the best performance, we need to submit requests
			 * as long as there is a slot available.
			 */
			num_iowait++;
			ctx->io_wait(NULL, 1);
			slot = ctx->max_io_slot();
		}
		struct iocb *reqs[slot];
		int min = slot > num ? num : slot;
		int num_iocb = 0;
		for (int i = 0; i < min; i++) {
			assert(requests->get_io());
			struct iocb *req = construct_req(*requests, aio_callback);
			requests++;
			if (req)
				reqs[num_iocb++] = req;
		}
		if (num_iocb > 0)
			ctx->submit_io_request(reqs, num_iocb);
		num -= min;
	}
	if (status)
		for (int i = 0; i < num; i++)
			status[i] = IO_PENDING;
}

class aio_complete_thread: public thread
{
	thread_safe_FIFO_queue<thread_callback_s *> completed_reqs;
public:
	aio_complete_thread(int node_id): thread(std::string("aio_complete")
			+ itoa(node_id), node_id), completed_reqs(
			std::string("aio_complete_queue-") + itoa(node_id), node_id, 10240) {
	}
	void run() {
		int num = completed_reqs.get_num_entries();
		thread_callback_s *tcbs[num];
		int ret = completed_reqs.fetch(tcbs, num);
		assert(ret == num);
		process_completed_reqs(tcbs, ret);
	}

	int add_reqs(thread_callback_s *tcbs[], int num) {
		int ret = completed_reqs.add(tcbs, num);
		activate();
		return ret;
	}

	static void process_completed_reqs(thread_callback_s *tcbs[], int num);
};

std::vector<aio_complete_thread *> complete_thread_table;

void init_aio(std::vector<int> node_ids)
{
	for (unsigned i = 0; i < node_ids.size(); i++) {
		int node_id = node_ids[i];
		if ((int) complete_thread_table.size() <= node_id)
			complete_thread_table.resize(node_id + 1);
		complete_thread_table[node_id] = new aio_complete_thread(node_id);
		complete_thread_table[node_id]->start();
	}
}

void destroy_aio()
{
	// TODO
}

void aio_complete_thread::process_completed_reqs(thread_callback_s *tcbs[],
		int num)
{
	// If the number of completed requests is small, it's unlikely that they
	// are from the same IO instance.
	if (num < 8) {
		for (int i = 0; i < num; i++) {
			thread_callback_s *tcb = tcbs[i];
			io_interface *io = tcb->req.get_io();
			io_request *req = &tcb->req;
			io->notify_completion(&req, 1);
			tcbs[i]->cb_allocator->free(tcbs[i]);
		}
	}
	else {
		// We should try to invoke for as many requests as possible,
		// so the upper layer has the opportunity to optimize the request completion.
		std::unordered_map<io_interface *, std::vector<io_request *> > map;
		for (int i = 0; i < num; i++) {
			thread_callback_s *tcb = tcbs[i];
			std::vector<io_request *> *v;
			std::unordered_map<io_interface *, std::vector<io_request *> >::iterator it;
			io_interface *io = tcb->req.get_io();
			if ((it = map.find(io)) == map.end()) {
				map.insert(std::pair<io_interface *, std::vector<io_request *> >(
							io, std::vector<io_request *>()));
				v = &map[io];
			}
			else
				v = &it->second;

			v->push_back(&tcb->req);
		}
		for (std::unordered_map<io_interface *, std::vector<io_request *> >::iterator it
				= map.begin(); it != map.end(); it++) {
			io_interface *io = it->first;
			std::vector<io_request *> *v = &it->second;
			io->notify_completion(v->data(), v->size());
		}
		for (int i = 0; i < num; i++) {
			tcbs[i]->cb_allocator->free(tcbs[i]);
		}
	}
}

void async_io::return_cb(thread_callback_s *tcbs[], int num)
{
	thread_callback_s *local_tcbs[num];
	thread_callback_s *remote_tcbs[num];
	int num_local = 0;
	int num_remote = 0;

	num_completed_reqs += num;
	for (int i = 0; i < num; i++) {
		thread_callback_s *tcb = tcbs[i];
		if (tcb->req.get_io() == this)
			local_tcbs[num_local++] = tcb;
		else
			remote_tcbs[num_remote++] = tcb;
	}
	if (num_local > 0) {
		io_request *reqs[num_local];
		for (int i = 0; i < num_local; i++)
			reqs[i] = &local_tcbs[i]->req;
		notify_completion(reqs, num_local);
		for (int i = 0; i < num_local; i++) {
			local_tcbs[i]->cb_allocator->free(local_tcbs[i]);
		}
	}
	if (num_remote > 0) {
		if (complete_thread_table.empty())
			aio_complete_thread::process_completed_reqs(remote_tcbs, num_remote);
		else {
			int ret = complete_thread_table[get_node_id()]->add_reqs(
					remote_tcbs, num_remote);
			aio_complete_thread::process_completed_reqs(remote_tcbs + ret,
					num_remote - ret);
		}
	}
}

void async_io::notify_completion(io_request *reqs[], int num)
{
	if (this->cb) {
		this->cb->invoke(reqs, num);
	}
}

int async_io::open_file(const logical_file_partition &partition)
{
	int file_id = partition.get_file_id();
	auto it = open_files.find(file_id);
	if (it == open_files.end()) {
		buffered_io *io = new buffered_io(partition, get_thread(),
				get_header(), O_DIRECT | open_flags);
		open_files.insert(std::pair<int, io_ref>(file_id, io_ref(io)));
#if 0
		if (data)
			data->add_new_file(io);
#endif
	}
	// The file has been opened.
	else if (it->second.is_valid()) {
		it->second.inc_ref();
	}
	// The file has been opened but was closed.
	else {
		it->second = io_ref(new buffered_io(partition, get_thread(),
					get_header(), O_DIRECT | open_flags));
	}
	return 0;
}

int async_io::close_file(int file_id)
{
	auto it = open_files.find(file_id);
	// Users shouldn't close a file that hasn't been opened before.
	assert(it != open_files.end());
	it->second.dec_ref();
//	open_files.erase(it);
	return 0;
}

void async_io::flush_requests()
{
}

const int AIO_NUM_PROCESS_REQS = AIO_DEPTH_PER_FILE * 16;

}
