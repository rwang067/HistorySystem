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

#include <stdio.h>
#include <stdlib.h>
#ifdef USE_NUMA
#include <numa.h>
#endif
#include <pthread.h>

#include <system_error>

#include "thread.h"
#include "common.h"

#ifdef USE_HWLOC

std::vector<hwloc_obj_t> get_objs_by_type(hwloc_obj_t obj, hwloc_obj_type_t type)
{
	if (obj->arity > 0 && obj->first_child->type == type) {
		std::vector<hwloc_obj_t> cores(obj->arity);
		for (size_t i = 0; i < cores.size(); i++)
			cores[i] = obj->children[i];
		return cores;
	}
	if (obj->arity == 0)
		return std::vector<hwloc_obj_t>();
	else {
		std::vector<hwloc_obj_t> cores;
		for (size_t i = 0; i < obj->arity; i++) {
			std::vector<hwloc_obj_t> tmp = get_objs_by_type(obj->children[i],
					type);
			cores.insert(cores.end(), tmp.begin(), tmp.end());
		}
		return cores;
	}
}

CPU_core::CPU_core(hwloc_obj_t core)
{
	std::vector<hwloc_obj_t> pus = get_objs_by_type(core,
			HWLOC_OBJ_PU);
	logical_units.resize(pus.size());
	for (size_t i = 0; i < logical_units.size(); i++)
		logical_units[i] = pus[i]->os_index;
}

NUMA_node::NUMA_node(hwloc_obj_t node)
{
	std::vector<hwloc_obj_t> hwloc_cores = get_objs_by_type(node,
			HWLOC_OBJ_CORE);
	for (size_t i = 0; i < hwloc_cores.size(); i++)
		cores.emplace_back(hwloc_cores[i]);
	std::vector<int> lu_vec = get_logical_units();
	lus.insert(lu_vec.begin(), lu_vec.end());
}

NUMA_node::NUMA_node(hwloc_topology_t topology)
{
	int num_cores = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_CORE);
	assert(num_cores > 0);
	for (int i = 0; i < num_cores; i++) {
		hwloc_obj_t core = hwloc_get_obj_by_type(topology,
				HWLOC_OBJ_CORE, i);
		cores.emplace_back(core);
	}
	std::vector<int> lu_vec = get_logical_units();
	lus.insert(lu_vec.begin(), lu_vec.end());
}

std::vector<int> NUMA_node::get_logical_units() const
{
	std::vector<int> ret;
	for (size_t i = 0; i < get_num_cores(); i++) {
		std::vector<int> units = get_core(i).get_units();
		ret.insert(ret.end(), units.begin(), units.end());
	}
	return ret;
}

CPU_hierarchy::CPU_hierarchy()
{
	hwloc_topology_t topology;
	hwloc_topology_init(&topology);
	hwloc_topology_load(topology);
	int num_nodes = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_NODE);
	if (num_nodes > 0) {
		for (int i = 0; i < num_nodes; i++) {
			hwloc_obj_t node = hwloc_get_obj_by_type(topology,
					HWLOC_OBJ_NODE, i);
			nodes.emplace_back(node);
		}
	}
	else
		nodes.emplace_back(topology);
}

std::vector<int> CPU_hierarchy::lus2node(const std::vector<int> &lus) const
{
	std::vector<int> ret(lus.size(), -1);
	for (size_t i = 0; i < get_num_nodes(); i++) {
		const NUMA_node &node = get_node(i);
		for (size_t j = 0; j < lus.size(); j++) {
			if (node.contain_lu(lus[j])) {
				assert(ret[j] == -1);
				ret[j] = i;
			}
		}
	}
	return ret;
}

CPU_hierarchy cpus;

#endif

static void bind2node_id(int node_id)
{
#ifdef USE_NUMA
	struct bitmask *bmp = numa_allocate_nodemask();
	numa_bitmask_setbit(bmp, node_id);
	numa_bind(bmp);
	numa_free_nodemask(bmp);
#endif
}

void *thread_run(void *arg)
{
	thread *t = (thread *) arg;
	pthread_setspecific(thread::thread_key, t);
	t->tid = gettid();

	std::vector<int> cpus = t->get_cpu_affinity();
	int node_id = t->get_node_id();
	if (!cpus.empty()) {
		cpu_set_t set;
		CPU_ZERO(&set);
		for (size_t i = 0; i < cpus.size(); i++)
			CPU_SET(cpus[i] + 1, &set);
		if (sched_setaffinity(t->tid, sizeof(set), &set) == -1)
			fprintf(stderr, "can't set CPU affinity on thread %d\n", t->tid);
	}
	else if (node_id >= 0)
		bind2node_id(node_id);

	t->init();
	while (t->is_running()) {
		t->run();
		if (t->is_blocking())
			t->wait();
	}
	t->cleanup();
	t->exit();
	return NULL;
}

void thread::construct_init()
{
	tid = -1;
	thread_idx = num_threads.inc(1);
	this->id = 0;

	_is_activated = false;
	_has_exit = false;
	_is_running = true;
	_is_sleeping = true;

	pthread_mutex_init(&mutex, NULL);
	pthread_cond_init(&cond, NULL);
	user_data = NULL;
}

thread::thread(std::string name, int node_id, bool blocking)
{
	thread_class_init();
	construct_init();

	this->node_id = node_id;
	this->name = name + "-" + itoa(thread_idx);
	this->blocking = blocking;
}

thread::thread(std::string name, const std::vector<int> &cpu_affinity,
		bool blocking)
{
	thread_class_init();
	construct_init();

#ifdef USE_HWLOC
	std::vector<int> node_ids = cpus.lus2node(cpu_affinity);
	this->node_id = node_ids.front();
	for (size_t i = 1; i < cpu_affinity.size(); i++)
		assert(node_id == node_ids[i]);
#else
	this->node_id = 0;
#endif
	this->cpu_affinity = cpu_affinity;
	this->name = name + "-" + itoa(thread_idx);
	this->blocking = blocking;
}

void thread::start()
{
	assert(id == 0);
	int ret = pthread_create(&id, NULL, thread_run, (void *) this);
	if (ret)
		throw std::system_error(std::make_error_code((std::errc) ret),
				"can't create thread");
}

static pthread_once_t once_control = PTHREAD_ONCE_INIT;

void init_thread_class()
{
	pthread_key_create(&thread::thread_key, NULL);
}

void thread::thread_class_init()
{
	pthread_once(&once_control, init_thread_class);
}

pthread_key_t thread::thread_key;

class thread_representer: public thread
{
	void run() {
	}
public:
	thread_representer(int node_id): thread(std::string(
				"representer-thread-node") + itoa(node_id), node_id) {
	}
};

thread *thread::get_curr_thread()
{
	thread *curr = (thread *) pthread_getspecific(thread_key);
	if (curr)
		return curr;
	// If the thread isn't associated with a thread object, we'll create
	// a thread representer just to make the other SAFS code work.
	else {
		curr = new thread_representer(-1);
		pthread_setspecific(thread::thread_key, curr);
		curr->_is_sleeping = false;
		curr->tid = gettid();
		return curr;
	}
}

thread *thread::represent_thread(int node_id)
{
	if (node_id >= 0)
		bind2node_id(node_id);

	thread *curr = thread::get_curr_thread();
	assert(curr == NULL);
	curr = new thread_representer(node_id);
	pthread_setspecific(thread::thread_key, curr);
	curr->_is_sleeping = false;
	curr->tid = gettid();
	return curr;
}

void task_thread::run()
{
	const int TASK_BUF_SIZE = 128;
	thread_task *local_tasks[TASK_BUF_SIZE];
	size_t tot_num = 0;
	pthread_mutex_lock(&mutex);
	while (!tasks.is_empty()) {
		int num_tasks = tasks.fetch(local_tasks, TASK_BUF_SIZE);
		pthread_mutex_unlock(&mutex);
		for (int i = 0; i < num_tasks; i++) {
			local_tasks[i]->run();
			delete local_tasks[i];
			num_pending--;
		}
		pthread_mutex_lock(&mutex);
		tot_num += num_tasks;
	}
	all_complete = true;
	pthread_mutex_unlock(&mutex);
	pthread_cond_signal(&cond);
}

void task_thread::wait4complete()
{
	pthread_mutex_lock(&mutex);
	while (!all_complete) {
		pthread_cond_wait(&cond, &mutex);
	}
	pthread_mutex_unlock(&mutex);
}

void task_thread::add_task(thread_task *t)
{
	pthread_mutex_lock(&mutex);
	all_complete = false;
	if (tasks.is_full())
		tasks.expand_queue(tasks.get_size() * 2);
	tasks.push_back(t);
	pthread_mutex_unlock(&mutex);
	activate();
	num_pending++;
}

atomic_integer thread::num_threads;
