/*
 * Copyright 2015 Open Connectome Project (http://openconnecto.me)
 * Written by Da Zheng (zhengda1936@gmail.com)
 *
 * This file is part of FlashMatrix.
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

#include "EM_object.h"
#include "mem_worker_thread.h"

namespace fm
{

namespace detail
{

EM_object::file_holder::ptr EM_object::file_holder::create_temp(
		const std::string &name, size_t num_bytes,
		safs::safs_file_group::ptr group)
{
	const size_t MAX_TRIES = 10;
	size_t i;
	std::string tmp_name;
	for (i = 0; i < MAX_TRIES; i++) {
		tmp_name = name + gen_rand_name(8);
		safs::safs_file f(safs::get_sys_RAID_conf(), tmp_name);
		if (!f.exist())
			break;
	}
	if (i == MAX_TRIES) {
		BOOST_LOG_TRIVIAL(error)
			<< "Can't create a temp matrix name because max tries are reached";
		return EM_object::file_holder::ptr();
	}

	safs::safs_file f(safs::get_sys_RAID_conf(), tmp_name);
	bool ret = f.create_file(num_bytes, safs::params.get_RAID_block_size(),
			safs::params.get_RAID_mapping_option(), group);
	if (!ret)
		return file_holder::ptr();
	else
		return file_holder::ptr(new file_holder(tmp_name, false));
}

EM_object::file_holder::ptr EM_object::file_holder::create(
		const std::string &name)
{
	safs::safs_file f(safs::get_sys_RAID_conf(), name);
	if (!f.exist())
		return ptr();
	else
		return ptr(new file_holder(name, true));
}

EM_object::file_holder::~file_holder()
{
	if (!persistent) {
		safs::safs_file f(safs::get_sys_RAID_conf(), file_name);
		if (f.exist())
			f.delete_file();
		else
			BOOST_LOG_TRIVIAL(error) << file_name << " doesn't exist any more";
	}
}

bool EM_object::file_holder::set_persistent(const std::string &new_name)
{
	auto ret = new_name.find("/");
	if (ret != std::string::npos) {
		fprintf(stderr, "can't rename to %s because it contains '/'\n",
				new_name.c_str());
		return false;
	}

	safs::safs_file f(safs::get_sys_RAID_conf(), file_name);
	if (!f.rename(new_name))
		return false;
	persistent = true;
	this->file_name = new_name;
	return true;
}

void EM_object::file_holder::unset_persistent()
{
	persistent = false;
}

safs::io_interface::ptr EM_object::io_set::create_io()
{
	thread *t = thread::get_curr_thread();
	assert(t);
	pthread_spin_lock(&io_lock);
	auto it = thread_ios.find(t);
	if (it == thread_ios.end()) {
		pthread_spin_unlock(&io_lock);

		safs::io_interface::ptr io = safs::create_io(factory, t);
		io->set_callback(portion_callback::ptr(new portion_callback()));
		pthread_setspecific(io_key, io.get());

		pthread_spin_lock(&io_lock);
		thread_ios.insert(std::pair<thread *, safs::io_interface::ptr>(t, io));
		pthread_spin_unlock(&io_lock);
		return io;
	}
	else {
		safs::io_interface::ptr io = it->second;
		pthread_spin_unlock(&io_lock);
		return io;
	}
}

EM_object::io_set::io_set(safs::file_io_factory::shared_ptr factory)
{
	this->factory = factory;
	int ret = pthread_key_create(&io_key, NULL);
	assert(ret == 0);
	pthread_spin_init(&io_lock, PTHREAD_PROCESS_PRIVATE);
}

EM_object::io_set::~io_set()
{
	pthread_spin_destroy(&io_lock);
	pthread_key_delete(io_key);
	thread_ios.clear();
}

bool EM_object::io_set::has_io() const
{
	return pthread_getspecific(io_key) != NULL;
}

safs::io_interface &EM_object::io_set::get_curr_io() const
{
	void *io_addr = pthread_getspecific(io_key);
	if (io_addr)
		return *(safs::io_interface *) io_addr;
	else {
		safs::io_interface::ptr io = const_cast<io_set *>(this)->create_io();
		return *io;
	}
}

void portion_callback::add(long key, portion_compute::ptr compute)
{
	auto it = computes.find(key);
	if (it == computes.end()) {
		std::vector<portion_compute::ptr> tmp(1);
		tmp[0] = compute;
		auto ret = computes.insert(
				std::pair<long, std::vector<portion_compute::ptr> >(key, tmp));
		assert(ret.second);
	}
	else
		it->second.push_back(compute);
}

int portion_callback::invoke(safs::io_request *reqs[], int num)
{
	for (int i = 0; i < num; i++) {
		auto it = computes.find(get_portion_key(*reqs[i]));
		// Sometimes we want to use the I/O instance synchronously, and
		// we don't need to keep a compute here.
		if (it == computes.end())
			continue;

		std::vector<portion_compute::ptr> tmp = it->second;
		// We have to remove the vector of computes from the hashtable first.
		// Otherwise, the user-defined `run' function may try to add new
		// computes to the vector.
		computes.erase(it);

		for (auto comp_it = tmp.begin(); comp_it != tmp.end(); comp_it++) {
			portion_compute::ptr compute = *comp_it;
			compute->run(reqs[i]->get_buf(), reqs[i]->get_size());
		}
	}
	return 0;
}

bool EM_portion_dispatcher::issue_task()
{
	pthread_spin_lock(&lock);
	off_t global_start = portion_idx * portion_size;
	if ((size_t) global_start >= tot_len) {
		pthread_spin_unlock(&lock);
		return false;
	}

	// If we have many portions remaining, we can issue a task with multiple
	// portions. However, if there remains only a small number of portions,
	// we should return one portion at a time to achieve better load balancing.
	size_t num_remain = div_ceil(tot_len - portion_size * portion_idx,
			portion_size);
	if (num_remain < balance_thres)
		num_portions_task = 1;

	size_t length = std::min(portion_size * num_portions_task,
			tot_len - global_start);
	portion_idx += div_ceil(length, portion_size);
	pthread_spin_unlock(&lock);
	create_task(global_start, length);
	return true;
}

}

}
