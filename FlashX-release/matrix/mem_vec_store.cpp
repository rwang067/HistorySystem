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
#include <boost/format.hpp>

#include "log.h"
#include "common.h"

#include "mem_vec_store.h"
#include "bulk_operate.h"
#include "local_vec_store.h"
#include "mem_matrix_store.h"
#include "NUMA_vector.h"

namespace fm
{

namespace detail
{

mem_vec_store::ptr mem_vec_store::create(size_t length, int num_nodes,
		const scalar_type &type)
{
	if (num_nodes < 0)
		return smp_vec_store::create(length, type);
	else
		return NUMA_vec_store::create(length, num_nodes, type);
}

bool mem_vec_store::copy_from(const char *buf, size_t num_bytes)
{
	if (get_raw_arr() == NULL)
		return false;

	if (num_bytes % get_entry_size() != 0
			|| num_bytes / get_entry_size() != get_length())
		return false;
	memcpy(get_raw_arr(), buf, num_bytes);
	return true;
}

smp_vec_store::smp_vec_store(size_t length, const scalar_type &type): mem_vec_store(
		length, type), data(length * type.get_size(), -1)
{
	this->arr = data.get_raw();
}

smp_vec_store::smp_vec_store(const detail::simple_raw_array &data,
		size_t length, const scalar_type &type): mem_vec_store(length, type)
{
	this->data = data;
	this->arr = this->data.get_raw();
}

smp_vec_store::ptr smp_vec_store::create(const detail::simple_raw_array &data,
			const scalar_type &type)
{
	if (data.get_num_bytes() % type.get_size() != 0) {
		BOOST_LOG_TRIVIAL(error)
			<< "The data array has a wrong number of bytes";
		return smp_vec_store::ptr();
	}
	return ptr(new smp_vec_store(data,
				data.get_num_bytes() / type.get_size(), type));
}

smp_vec_store::ptr smp_vec_store::create(const detail::simple_raw_array &data,
			size_t length, const scalar_type &type)
{
	if (length * type.get_size() > data.get_num_bytes()) {
		BOOST_LOG_TRIVIAL(error)
			<< "The data array doesn't have enough bytes for the vector";
		return smp_vec_store::ptr();
	}
	return ptr(new smp_vec_store(data, length, type));
}

smp_vec_store::ptr smp_vec_store::get(const smp_vec_store &idxs) const
{
	if (idxs.get_type() != get_scalar_type<off_t>()) {
		BOOST_LOG_TRIVIAL(error) << "The index vector isn't of the off_t type";
		return smp_vec_store::ptr();
	}

	smp_vec_store::ptr ret = smp_vec_store::create(idxs.get_length(), get_type());
	int num_threads = get_num_omp_threads();
	size_t part_len = ceil(((double) idxs.get_length()) / num_threads);
#pragma omp parallel for
	for (int k = 0; k < num_threads; k++) {
		size_t start = k * part_len;
		size_t end = std::min((k + 1) * part_len, idxs.get_length());
		size_t local_len = end >= start ? end - start : 0;
		if (local_len == 0)
			continue;

		std::vector<const char *> src_locs(local_len);
		for (size_t i = 0; i < local_len; i++) {
			off_t idx = idxs.get<off_t>(i + start);
			// Check if it's out of the range.
			if (idx < 0 && (size_t) idx >= this->get_length()) {
				BOOST_LOG_TRIVIAL(error)
					<< boost::format("%1% is out of range") % idx;
				continue;
			}

			src_locs[i] = this->get(idx);
		}
		get_type().get_sg().gather(src_locs,
				ret->arr + start * ret->get_entry_size());
	}
	return ret;
}

bool smp_vec_store::append(std::vector<vec_store::const_ptr>::const_iterator vec_it,
		std::vector<vec_store::const_ptr>::const_iterator vec_end)
{
	// Get the total size of the result vector.
	size_t tot_res_size = this->get_length();
	for (auto it = vec_it; it != vec_end; it++) {
		tot_res_size += (*it)->get_length();
		if (!(*it)->is_in_mem()) {
			BOOST_LOG_TRIVIAL(error)
				<< "Not support appending an ext-mem vector to an in-mem vector";
			return false;
		}
		if (get_type() != (*it)->get_type()) {
			BOOST_LOG_TRIVIAL(error) << "The two vectors don't have the same type";
			return false;
		}
	}

	// Merge all results to a single vector.
	off_t loc = this->get_length() + get_sub_start();
	this->resize(tot_res_size);
	for (auto it = vec_it; it != vec_end; it++) {
		assert(loc + (*it)->get_length() <= this->get_length());
		assert((*it)->is_in_mem());
		const mem_vec_store &mem_vec = static_cast<const mem_vec_store &>(**it);
		bool ret = data.set_sub_arr(loc * get_entry_size(),
				mem_vec.get_raw_arr(), mem_vec.get_length() * get_entry_size());
		assert(ret);
		loc += (*it)->get_length();
	}
	return true;
}

bool smp_vec_store::append(const vec_store &vec)
{
	if (!vec.is_in_mem()) {
		BOOST_LOG_TRIVIAL(error) << "The input vector isn't in memory";
		return false;
	}
	if (get_type() != vec.get_type()) {
		BOOST_LOG_TRIVIAL(error) << "The two vectors don't have the same type";
		return false;
	}
	// TODO We might want to over expand a little, so we don't need to copy
	// the memory again and again.
	// TODO if this is a sub_vector, what should we do?
	assert(get_sub_start() == 0);
	off_t loc = this->get_length() + get_sub_start();
	this->resize(vec.get_length() + get_length());
	assert(loc + vec.get_length() <= this->get_length());
	const mem_vec_store &mem_vec = static_cast<const mem_vec_store &>(vec);
	assert(mem_vec.get_raw_arr());
	return data.set_sub_arr(loc * get_entry_size(), mem_vec.get_raw_arr(),
			mem_vec.get_length() * get_entry_size());
}

size_t smp_vec_store::get_reserved_size() const
{
	return data.get_num_bytes() / get_type().get_size();
}

bool smp_vec_store::reserve(size_t new_length)
{
	if (new_length == get_length())
		return true;

	size_t tot_len = data.get_num_bytes() / get_type().get_size();
	if (new_length < tot_len)
		return true;

	// Keep the old information of the vector.
	detail::simple_raw_array old_data = data;
	char *old_arr = arr;
	size_t old_length = get_length();

	size_t real_length = old_length;
	if (real_length == 0)
		real_length = 1;
	for (; real_length < new_length; real_length *= 2);
	this->data = detail::simple_raw_array(real_length * get_type().get_size(), -1);
	this->arr = this->data.get_raw();
	memcpy(arr, old_arr, std::min(old_length, new_length) * get_type().get_size());
	return true;
}

bool smp_vec_store::resize(size_t new_length)
{
	if (new_length == get_length())
		return true;

	size_t tot_len = data.get_num_bytes() / get_type().get_size();
	// We don't want to reallocate memory when shrinking the vector.
	if (new_length <= tot_len) {
		return vec_store::resize(new_length);
	}

	// Keep the old information of the vector.
	detail::simple_raw_array old_data = data;
	char *old_arr = arr;
	size_t old_length = get_length();

	size_t real_length = old_length;
	if (real_length == 0)
		real_length = 1;
	for (; real_length < new_length; real_length *= 2);
	this->data = detail::simple_raw_array(real_length * get_type().get_size(), -1);
	this->arr = this->data.get_raw();
	memcpy(arr, old_arr, std::min(old_length, new_length) * get_type().get_size());
	return vec_store::resize(new_length);
}

bool smp_vec_store::expose_sub_vec(off_t start, size_t length)
{
	size_t tot_len = data.get_num_bytes() / get_type().get_size();
	if (start + length > tot_len) {
		BOOST_LOG_TRIVIAL(error) << "expose_sub_vec: out of range";
		return false;
	}

	resize(length);
	arr = data.get_raw() + start * get_entry_size();
	return true;
}

vec_store::ptr smp_vec_store::deep_copy() const
{
	assert(get_raw_arr() == data.get_raw());
	detail::simple_raw_array copy = this->data.deep_copy();
	smp_vec_store::ptr ret = smp_vec_store::create(copy, get_type());
	ret->resize(this->get_length());
	return ret;
}

vec_store::ptr smp_vec_store::sort_with_index()
{
	smp_vec_store::ptr indexes = smp_vec_store::create(get_length(),
			get_scalar_type<off_t>());
	get_type().get_sorter().sort_with_index(arr,
			(off_t *) indexes->arr, get_length(), false);
	return indexes;
}

void smp_vec_store::set_data(const set_vec_operate &op)
{
	// I assume this is column-wise matrix.
	// TODO parallel.
	op.set(arr, get_length(), 0);
}

void smp_vec_store::set(const std::vector<const char *> &locs)
{
	assert(locs.size() <= get_length());
	get_type().get_sg().gather(locs, arr);
}

bool smp_vec_store::set_portion(std::shared_ptr<const local_vec_store> store,
		off_t loc)
{
	if (store->get_type() != get_type()) {
		BOOST_LOG_TRIVIAL(error) << "The input store has a different type";
		return false;
	}
	if (loc + store->get_length() > get_length()) {
		BOOST_LOG_TRIVIAL(error) << "out of boundary";
		return false;
	}
	size_t entry_size = get_type().get_size();
	memcpy(arr + loc * entry_size, store->get_raw_arr(),
			store->get_length() * entry_size);
	return true;
}

local_vec_store::ptr smp_vec_store::get_portion(off_t loc, size_t size)
{
	if (loc + size > get_length()) {
		BOOST_LOG_TRIVIAL(error) << "the portion is out of boundary";
		return local_vec_store::ptr();
	}

	return local_vec_store::ptr(new local_ref_vec_store(get(loc),
				loc, size, get_type(), -1));
}

local_vec_store::const_ptr smp_vec_store::get_portion(off_t loc,
			size_t size) const
{
	if (loc + size > get_length()) {
		BOOST_LOG_TRIVIAL(error) << "the portion is out of boundary";
		return local_vec_store::ptr();
	}

	return local_vec_store::ptr(new local_cref_vec_store(get(loc),
				loc, size, get_type(), -1));
}

size_t smp_vec_store::get_portion_size() const
{
	return 64 * 1024;
}

matrix_store::ptr smp_vec_store::conv2mat(size_t nrow, size_t ncol, bool byrow)
{
	assert(arr == data.get_raw());
	if (get_length() < nrow * ncol) {
		BOOST_LOG_TRIVIAL(error)
			<< "The vector doesn't have enough elements";
		return matrix_store::ptr();
	}
	if (byrow)
		return mem_row_matrix_store::create(data, nrow, ncol, get_type());
	else
		return mem_col_matrix_store::create(data, nrow, ncol, get_type());
}

}

}
