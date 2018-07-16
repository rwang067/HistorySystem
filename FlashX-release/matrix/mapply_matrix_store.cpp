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

#include "mapply_matrix_store.h"
#include "local_matrix_store.h"
#include "vec_store.h"
#include "local_mem_buffer.h"
#include "materialize.h"
#include "EM_dense_matrix.h"

namespace fm
{

namespace detail
{

class materialized_mapply_tall_store
{
	matrix_store::const_ptr tall_res;
	matrix_store::const_ptr wide_res;

	std::pair<size_t, size_t> portion_size;
	// The buffer matrix that keep the full materialization result.
	matrix_store::ptr res_buf;
	// The number of elements that been written.
	global_counter num_res_avails;
public:
	typedef std::shared_ptr<materialized_mapply_tall_store> ptr;

	materialized_mapply_tall_store(matrix_store::ptr res);
	materialized_mapply_tall_store(matrix_store::const_ptr res) {
		set_materialized(res);
	}

	size_t get_num_rows() const {
		assert(tall_res);
		return tall_res->get_num_rows();
	}

	size_t get_num_cols() const {
		assert(tall_res);
		return tall_res->get_num_cols();
	}

	/*
	 * The materialized matrix should have the same layout as the mapply matrix.
	 * It's not ambiguous to use the layout to figure out the right matrix.
	 * In contrast, is_wide() can be ambiguous when it's a square matrix.
	 */
	const matrix_store &get_materialize_ref(matrix_layout_t layout) {
		if (layout == wide_res->store_layout())
			return *wide_res;
		else
			return *tall_res;
	}

	matrix_store::const_ptr get_materialize_res(matrix_layout_t layout) {
		if (layout == wide_res->store_layout())
			return wide_res;
		else
			return tall_res;
	}

	void set_materialized(matrix_store::const_ptr res);

	bool is_in_mem() const {
		return tall_res->is_in_mem();
	}

	bool is_materialized() const {
		size_t num_eles = get_num_rows() * get_num_cols();
		size_t count = num_res_avails.get();
		assert(num_eles >= count);
		return count == num_eles;
	}

	void write_portion_async(local_matrix_store::const_ptr portion,
			size_t start_row, size_t start_col);

	local_matrix_store::ptr get_portion(size_t start_row, size_t start_col,
			size_t num_rows, size_t num_cols) {
		num_res_avails.inc(num_rows * num_cols);
		return res_buf->get_portion(start_row, start_col, num_rows, num_cols);
	}

	void print_state() const {
		if (is_materialized())
			BOOST_LOG_TRIVIAL(info) << "is fully materialized";
		else
			BOOST_LOG_TRIVIAL(info) << boost::format(
					"there are %1% portions materialized")
				% num_res_avails.get();
	}
};

materialized_mapply_tall_store::materialized_mapply_tall_store(
		matrix_store::ptr buf)
{
	// We need to make sure the buffer matrix is tall.
	assert(!buf->is_wide());
	res_buf = buf;
	portion_size = res_buf->get_portion_size();
	tall_res = res_buf;
	wide_res = res_buf->transpose();
}

void materialized_mapply_tall_store::set_materialized(matrix_store::const_ptr res)
{
	if (res->is_wide()) {
		tall_res = res->transpose();
		wide_res = res;
	}
	else {
		tall_res = res;
		wide_res = res->transpose();
	}
	portion_size = tall_res->get_portion_size();
	res_buf = NULL;
	num_res_avails.reset();
	num_res_avails.inc(res->get_num_rows() * res->get_num_cols());
}

void materialized_mapply_tall_store::write_portion_async(
		local_matrix_store::const_ptr portion, size_t start_row,
		size_t start_col)
{
	assert(portion->get_num_cols() == portion_size.second);
	res_buf->write_portion_async(portion, start_row, start_col);
	num_res_avails.inc(portion->get_num_rows() * portion->get_num_cols());
}

namespace
{

class mapply_store
{
	materialize_level mater_level;
	std::vector<local_matrix_store::const_ptr> ins;
	const portion_mapply_op &op;
	// This is the global matrix store that keeps the materialized result.
	materialized_mapply_tall_store *global_res;
	// This stores the materialized result for the whole portion.
	// If it exists, we always get data from it.
	local_matrix_store::ptr whole_res;
	// This stores the materialized results for individual sub chunks.
	std::vector<local_matrix_store::ptr> res_bufs;
	local_matrix_store *lstore;

	// The partition size.
	size_t part_size;
	// The number of elements that have been materialized.
	size_t num_materialized_eles;
	// The information of the original portion.
	bool is_wide;
	size_t orig_global_start_row;
	size_t orig_global_start_col;
	size_t orig_num_rows;
	size_t orig_num_cols;

	bool is_right_part(const local_matrix_store &part) const {
		return part.get_global_start_row() == lstore->get_global_start_row()
			&& part.get_global_start_col() == lstore->get_global_start_col()
			&& part.get_num_rows() == lstore->get_num_rows()
			&& part.get_num_cols() == lstore->get_num_cols();
	}

	off_t get_part_idx() const {
		off_t part_idx;
		if (is_wide)
			part_idx = lstore->get_local_start_col() / part_size;
		else
			part_idx = lstore->get_local_start_row() / part_size;
		return part_idx;
	}
public:
	mapply_store(const std::vector<local_matrix_store::const_ptr> &ins,
			const portion_mapply_op &_op, materialized_mapply_tall_store *global_res,
			local_matrix_store *lstore, materialize_level mater_level): op(_op) {
		this->global_res = global_res;
		this->mater_level = mater_level;
		this->lstore = lstore;
		this->ins = ins;
		assert(ins.size() > 0);

		// This might be a better way of determining if the matrix is wide.
		// In some cases, the input matrices are wide, but the output matrix
		// is tall. An example is combining multiple matrices into a single
		// matrix.
		is_wide = ins.front()->is_wide();
		orig_global_start_row = lstore->get_global_start_row();
		orig_global_start_col = lstore->get_global_start_col();
		orig_num_rows = lstore->get_num_rows();
		orig_num_cols = lstore->get_num_cols();
		if (is_wide)
			part_size = lstore->get_num_cols();
		else
			part_size = lstore->get_num_rows();
		num_materialized_eles = 0;
	}

	const char *get_raw_arr() const {
		const char *ret = NULL;
		if (whole_res)
			ret = whole_res->get_raw_arr();
		else if (!lstore->is_whole() && res_bufs[get_part_idx()]
				&& is_right_part(*res_bufs[get_part_idx()]))
			ret = res_bufs[get_part_idx()]->get_raw_arr();
		// If the materialization level is CPU cache, the materialized data is
		// stored in the first buffer.
		else if (mater_level == materialize_level::MATER_CPU
				&& !lstore->is_whole() && res_bufs[0]
				&& is_right_part(*res_bufs[0])) {
			ret = res_bufs[0]->get_raw_arr();
		}
		return ret;
	}

	const local_matrix_store &get_whole_res() const {
		return *whole_res;
	}

	const local_matrix_store &get_materialized_res() const {
		// No matter we are in the resized subchunk or in the whole portion,
		// whole_res always has the right data if whole_res exists.
		if (whole_res)
			return *whole_res;
		else if (!lstore->is_whole() && res_bufs[get_part_idx()]
				&& is_right_part(*res_bufs[get_part_idx()]))
			return *res_bufs[get_part_idx()];
		else {
			assert(mater_level == materialize_level::MATER_CPU
					&& !lstore->is_whole() && res_bufs[0]
					&& is_right_part(*res_bufs[0]));
			return *res_bufs[0];
		}
	}

	bool is_materialized() const {
		// If the whole portion is materialized, it always returns true.
		if (whole_res
				// If the current part is materialized.
				|| (!lstore->is_whole() && res_bufs[get_part_idx()]
					&& is_right_part(*res_bufs[get_part_idx()]))
				// If we materialize the current part in CPU cache.
				|| (mater_level == materialize_level::MATER_CPU
					&& !lstore->is_whole() && res_bufs[0]
					&& is_right_part(*res_bufs[0])))
			return true;
		else
			return false;
	}
	void materialize() const;
	void materialize_whole();

	bool resize(off_t local_start_row, off_t local_start_col,
			size_t local_num_rows, size_t local_num_cols);
	void reset_size();
};

void mapply_store::reset_size()
{
	for (size_t i = 0; i < ins.size(); i++)
		const_cast<local_matrix_store *>(ins[i].get())->reset_size();

	if (whole_res)
		whole_res->reset_size();
}

bool mapply_store::resize(off_t local_start_row, off_t local_start_col,
		size_t local_num_rows, size_t local_num_cols)
{
	// When someone accesses the matrix, he might want to access it with
	// smaller partitions. If we haven't partitioned the matrix yet, we should
	// use this function to deterime the partition size. The first time that
	// someone wants to resize it, we assume the new size is the partition size.
	// The part size should 2^n.

	// If the new size is the same as the old size, we don't need to do anything.
	if (local_start_row == 0 && local_num_rows == lstore->get_num_rows()
			&& local_start_col == 0 && local_num_cols == lstore->get_num_cols())
		return true;

	// The portion operator contains some data. We need to make sure
	// the resize is allowed in the portion operator.
	if (!op.is_resizable(local_start_row, local_start_col, local_num_rows,
				local_num_cols))
		return false;

	// We can either resize rows.
	assert((local_start_row == 0 && local_num_rows == lstore->get_num_rows())
			// or resize cols.
			|| (local_start_col == 0 && local_num_cols == lstore->get_num_cols()));
	size_t num_parts = 0;
	// Here we resize rows.
	if (local_start_row == 0 && local_num_rows == lstore->get_num_rows()) {
		off_t orig_in_start_col = ins[0]->get_local_start_col();
		size_t orig_in_num_cols = ins[0]->get_num_cols();
		bool success = true;
		for (size_t i = 0; i < ins.size(); i++) {
			success = const_cast<local_matrix_store *>(ins[i].get())->resize(
					0, local_start_col, ins[i]->get_num_rows(), local_num_cols);
			if (!success)
				break;
		}
		// If we fail, we need to restore the original matrix size.
		if (!success) {
			local_matrix_store::exposed_area area;
			area.local_start_row = 0;
			area.local_start_col = orig_in_start_col;
			area.num_cols = orig_in_num_cols;
			for (size_t i = 0; i < ins.size(); i++) {
				area.num_rows = ins[i]->get_num_rows();
				const_cast<local_matrix_store *>(ins[i].get())->restore_size(
						area);
			}
			return false;
		}
		if (part_size == orig_num_cols && local_num_cols < orig_num_cols) {
			assert(local_start_col == 0);
			part_size = local_num_cols;
			num_parts = div_ceil<size_t>(orig_num_cols, part_size);
		}
	}
	// Here we resize cols.
	else {
		off_t orig_in_start_row = ins[0]->get_local_start_row();
		size_t orig_in_num_rows = ins[0]->get_num_rows();
		bool success = true;
		for (size_t i = 0; i < ins.size(); i++) {
			success = const_cast<local_matrix_store *>(ins[i].get())->resize(
					local_start_row, 0, local_num_rows, ins[i]->get_num_cols());
			if (!success)
				break;
		}
		// If we fail, we need to restore the original matrix size.
		if (!success) {
			local_matrix_store::exposed_area area;
			area.local_start_row = orig_in_start_row;
			area.local_start_col = 0;
			area.num_rows = orig_in_num_rows;
			for (size_t i = 0; i < ins.size(); i++) {
				area.num_cols = ins[i]->get_num_cols();
				const_cast<local_matrix_store *>(ins[i].get())->restore_size(
						area);
			}
			return false;
		}
		if (part_size == orig_num_rows && local_num_rows < orig_num_rows) {
			assert(local_start_row == 0);
			part_size = local_num_rows;
			num_parts = div_ceil<size_t>(orig_num_rows, part_size);
		}
	}
	if (whole_res) {
		bool ret = whole_res->resize(local_start_row, local_start_col,
				local_num_rows, local_num_cols);
		// Resize the buffer matrix should always succeed.
		assert(ret);
	}
	// We haven't partitioned the matrix yet. We now use the new partition size.
	if (num_parts > 0 && res_bufs.empty()) {
		assert(num_parts > 1);
		res_bufs.resize(num_parts);
	}
	return true;
}

/*
 * This is to materialize the entire portion of a mapply store.
 * When there are a chain of mapply operations, we need to break a mapply
 * portion into smaller portions, so that data in the chain can be retained
 * in the CPU cache as much as possible.
 */
void mapply_store::materialize_whole()
{
	if (whole_res)
		return;

	// If we need full materialization, we need to keep the materialized result.
	// We only do it if the materialized result is stored in memory.
	// As such, we get the portion from the materialized matrix store directly.
	if (mater_level == materialize_level::MATER_FULL && global_res
			&& global_res->is_in_mem()) {
		// TODO we need to check is_wide more explicitly.
		// is_wide in the local store doesn't necessarily mean is_wide in
		// the global matrix store.
		if (is_wide) {
			local_matrix_store::ptr tmp = global_res->get_portion(
					lstore->get_global_start_col(), lstore->get_global_start_row(),
					lstore->get_num_cols(), lstore->get_num_rows());
			whole_res = std::static_pointer_cast<local_matrix_store>(
					tmp->transpose());
		}
		else
			whole_res = global_res->get_portion(lstore->get_global_start_row(),
					lstore->get_global_start_col(), lstore->get_num_rows(),
					lstore->get_num_cols());
	}
	else if (lstore->store_layout() == matrix_layout_t::L_COL) {
		local_buf_col_matrix_store *tmp = new local_buf_col_matrix_store(
				lstore->get_global_start_row(),
				lstore->get_global_start_col(), lstore->get_num_rows(),
				lstore->get_num_cols(), lstore->get_type(), -1);
		whole_res = local_matrix_store::ptr(tmp);
	}
	else {
		local_buf_row_matrix_store *tmp = new local_buf_row_matrix_store(
				lstore->get_global_start_row(),
				lstore->get_global_start_col(), lstore->get_num_rows(),
				lstore->get_num_cols(), lstore->get_type(), -1);
		whole_res = local_matrix_store::ptr(tmp);
	}

	// If all parts in the local matrix store have been materialized,
	// we can merge them.
	if (num_materialized_eles == orig_num_rows * orig_num_cols && res_bufs[0]) {
		for (size_t i = 0; i < res_bufs.size(); i++)
			assert(res_bufs[i]);

		// Copy all the parts
		if (is_wide) {
			for (size_t local_start_col = 0; local_start_col < orig_num_cols;
					local_start_col += part_size) {
				size_t local_num_cols = std::min(part_size,
						orig_num_cols - local_start_col);
				whole_res->resize(0, local_start_col, whole_res->get_num_rows(),
						local_num_cols);
				off_t part_idx = local_start_col / part_size;
				assert(res_bufs[part_idx]);
				whole_res->copy_from(*res_bufs[part_idx]);
			}
		}
		else {
			// If this is a tall matrix.
			for (size_t local_start_row = 0; local_start_row < orig_num_rows;
					local_start_row += part_size) {
				size_t local_num_rows = std::min(part_size,
						orig_num_rows - local_start_row);
				whole_res->resize(local_start_row, 0, local_num_rows,
						whole_res->get_num_cols());
				off_t part_idx = local_start_row / part_size;
				assert(res_bufs[part_idx]);
				whole_res->copy_from(*res_bufs[part_idx]);
			}
		}
		whole_res->reset_size();
	}
	else {
		op.run(ins, *whole_res);
		num_materialized_eles
			= whole_res->get_num_rows() * whole_res->get_num_cols();
	}

	// We can clean up all the parts now if there is any. We'll always access
	// data from `whole_res' from now on.
	for (size_t i = 0; i < res_bufs.size(); i++)
		res_bufs[i] = NULL;
	// We don't need the input matrix portions any more.
	ins.clear();
}

void mapply_store::materialize() const
{
	mapply_store *mutable_this = const_cast<mapply_store *>(this);
	if (lstore->is_whole()) {
		mutable_this->materialize_whole();
		return;
	}

	// If we have materialized the whole portion.
	if (whole_res)
		return;

	// If we have materialized the part
	off_t part_idx = get_part_idx();
	if (res_bufs[part_idx] && is_right_part(*res_bufs[part_idx]))
		return;

	// Materialize the part.
	local_matrix_store::ptr part;
	// If we need full materialization, we need to keep the materialized result.
	// We only do it if the materialized result is stored in memory.
	// As such, we get the portion from the materialized matrix store directly.
	if (mater_level == materialize_level::MATER_FULL && global_res
			&& global_res->is_in_mem()) {
		// TODO we need to check is_wide more explicitly.
		// is_wide in the local store doesn't necessarily mean is_wide in
		// the global matrix store.
		if (is_wide) {
			local_matrix_store::ptr tmp = global_res->get_portion(
					lstore->get_global_start_col(), lstore->get_global_start_row(),
					lstore->get_num_cols(), lstore->get_num_rows());
			part = std::static_pointer_cast<local_matrix_store>(
					tmp->transpose());
		}
		else
			part = global_res->get_portion(lstore->get_global_start_row(),
					lstore->get_global_start_col(), lstore->get_num_rows(),
					lstore->get_num_cols());
	}
	else if (lstore->store_layout() == matrix_layout_t::L_COL)
		part = local_matrix_store::ptr(
				new local_buf_col_matrix_store(lstore->get_global_start_row(),
					lstore->get_global_start_col(), lstore->get_num_rows(),
					lstore->get_num_cols(), lstore->get_type(), -1));
	else
		part = local_matrix_store::ptr(
				new local_buf_row_matrix_store(lstore->get_global_start_row(),
					lstore->get_global_start_col(), lstore->get_num_rows(),
					lstore->get_num_cols(), lstore->get_type(), -1));
	op.run(ins, *part);
	// If the materialization level is CPU cache.
	if (mater_level == materialize_level::MATER_CPU)
		mutable_this->res_bufs[0] = part;
	else {
		mutable_this->num_materialized_eles
			+= part->get_num_rows() * part->get_num_cols();
		mutable_this->res_bufs[part_idx] = part;
	}

	if (num_materialized_eles == orig_num_rows * orig_num_cols)
		// We don't need the input matrix portions any more.
		mutable_this->ins.clear();
}

/*
 * This portion compute has two functions.
 * It collects multiple portions if the mapply store is on top of multiple
 * matrix stores. It can also collect multiple user's portion computation
 * if multiple users need the portion.
 */
class collect_portion_compute: public portion_compute
{
	size_t num_EM_parts;
	std::vector<portion_compute::ptr> orig_computes;
	local_matrix_store::const_ptr res;
	materialized_mapply_tall_store::ptr global_res;
	size_t num_reads;
	bool is_wide;
public:
	typedef std::shared_ptr<collect_portion_compute> ptr;

	collect_portion_compute(portion_compute::ptr orig_compute,
			materialized_mapply_tall_store::ptr global_res, bool is_wide) {
		this->num_EM_parts = 0;
		this->num_reads = 0;
		this->orig_computes.push_back(orig_compute);
		this->global_res = global_res;
		this->is_wide = is_wide;
	}

	size_t get_num_EM_parts() const {
		return num_EM_parts;
	}

	void add_EM_part(local_matrix_store::const_ptr part) {
		num_EM_parts++;
	}

	void add_orig_compute(portion_compute::ptr compute) {
		// When the portion compute is created, the user must have provide
		// his portion compute.
		assert(orig_computes.size() > 0);
		orig_computes.push_back(compute);
	}

	void set_res_part(local_matrix_store::ptr res) {
		this->res = res;
	}

	virtual void run(char *buf, size_t size);
};

void collect_portion_compute::run(char *buf, size_t size)
{
	num_reads++;
	if (num_reads == num_EM_parts) {
		size_t num_eles = res->get_num_rows() * res->get_num_cols();
		for (size_t i = 0; i < orig_computes.size(); i++) {
			// TODO this may be problematic.
			// char *raw = const_cast<char *>(res->get_raw_arr());
			orig_computes[i]->run(NULL, num_eles * res->get_entry_size());
		}
		// This only runs once.
		// Let's remove all user's portion compute to indicate that it has
		// been invoked.
		orig_computes.clear();

		// If we want full materialization, we should write the result back.
		// We don't need to do it explicitly for in-memory matrix because
		// we store the materialized portions to in-memory matrix directly.
		if (global_res && !global_res->is_in_mem()) {
			// If the portion is too large, we should split it and materialize
			// parts separately to improve CPU cache hits.
			if (res->get_num_rows() == EM_matrix_store::CHUNK_SIZE
					|| res->get_num_cols() == EM_matrix_store::CHUNK_SIZE) {
				std::vector<detail::local_matrix_store::const_ptr> lstores(1);
				lstores[0] = res;
				if (res->is_wide())
					materialize_wide(lstores);
				else
					materialize_tall(lstores);
			}
			// The global result may be in memory while the input matrices
			// are in external memory. In this case, the materialized data
			// is stored in memory automatically.
			if (is_wide) {
				local_matrix_store::const_ptr tres
					= std::static_pointer_cast<const local_matrix_store>(
						res->transpose());
				global_res->write_portion_async(tres,
						tres->get_global_start_row(), tres->get_global_start_col());
			}
			else
				global_res->write_portion_async(res,
						res->get_global_start_row(), res->get_global_start_col());
		}

		// We don't need to reference the result matrix portion any more.
		res = NULL;
	}
}

class lmapply_col_matrix_store: public lvirtual_col_matrix_store
{
	std::weak_ptr<collect_portion_compute> collect_compute;
	mapply_store store;
public:
	lmapply_col_matrix_store(materialize_level mater_level,
			const std::vector<local_matrix_store::const_ptr> &ins,
			const portion_mapply_op &op,
			materialized_mapply_tall_store *res,
			collect_portion_compute::ptr collect_compute,
			off_t global_start_row, off_t global_start_col,
			size_t nrow, size_t ncol, const scalar_type &type,
			int node_id): lvirtual_col_matrix_store(global_start_row,
				global_start_col, nrow, ncol, type, node_id),
			store(ins, op, res, this, mater_level) {
		this->collect_compute = collect_compute;
	}

	collect_portion_compute::ptr get_compute() const {
		return collect_compute.lock();
	}

	virtual bool resize(off_t local_start_row, off_t local_start_col,
			size_t local_num_rows, size_t local_num_cols) {
		bool ret = store.resize(local_start_row, local_start_col,
				local_num_rows, local_num_cols);
		if (!ret)
			return false;
		return local_matrix_store::resize(local_start_row, local_start_col,
				local_num_rows, local_num_cols);
	}
	virtual void reset_size() {
		store.reset_size();
		local_matrix_store::reset_size();
	}

	using lvirtual_col_matrix_store::get_raw_arr;
	virtual const char *get_raw_arr() const {
		materialize_self();
		return store.get_raw_arr();
	}

	using lvirtual_col_matrix_store::transpose;
	virtual matrix_store::const_ptr transpose() const {
		materialize_self();
		return store.get_materialized_res().transpose();
	}

	using lvirtual_col_matrix_store::get_col;
	virtual const char *get_col(size_t col) const {
		materialize_self();
		return static_cast<const local_col_matrix_store &>(
				store.get_materialized_res()).get_col(col);
	}

	virtual local_matrix_store::const_ptr get_portion(
			size_t local_start_row, size_t local_start_col, size_t num_rows,
			size_t num_cols) const {
		assert(is_whole());
		store.materialize();
		return store.get_whole_res().get_portion(local_start_row,
				local_start_col, num_rows, num_cols);
	}
	virtual void materialize_self() const {
		if (!store.is_materialized())
			store.materialize();
	}
};

class lmapply_row_matrix_store: public lvirtual_row_matrix_store
{
	std::weak_ptr<collect_portion_compute> collect_compute;
	mapply_store store;
public:
	lmapply_row_matrix_store(materialize_level mater_level,
			const std::vector<local_matrix_store::const_ptr> &ins,
			const portion_mapply_op &op,
			materialized_mapply_tall_store *res,
			collect_portion_compute::ptr collect_compute,
			off_t global_start_row, off_t global_start_col,
			size_t nrow, size_t ncol, const scalar_type &type,
			int node_id): lvirtual_row_matrix_store(global_start_row,
				global_start_col, nrow, ncol, type, node_id),
			store(ins, op, res, this, mater_level) {
		this->collect_compute = collect_compute;
	}

	collect_portion_compute::ptr get_compute() const {
		return collect_compute.lock();
	}

	virtual bool resize(off_t local_start_row, off_t local_start_col,
			size_t local_num_rows, size_t local_num_cols) {
		bool ret = store.resize(local_start_row, local_start_col,
				local_num_rows, local_num_cols);
		if (!ret)
			return false;
		return local_matrix_store::resize(local_start_row, local_start_col,
				local_num_rows, local_num_cols);
	}
	virtual void reset_size() {
		store.reset_size();
		local_matrix_store::reset_size();
	}

	using lvirtual_row_matrix_store::get_raw_arr;
	virtual const char *get_raw_arr() const {
		materialize_self();
		return store.get_raw_arr();
	}

	using lvirtual_row_matrix_store::transpose;
	virtual matrix_store::const_ptr transpose() const {
		materialize_self();
		return store.get_materialized_res().transpose();
	}

	using lvirtual_row_matrix_store::get_row;
	virtual const char *get_row(size_t row) const {
		materialize_self();
		return static_cast<const local_row_matrix_store &>(
				store.get_materialized_res()).get_row(row);
	}

	virtual local_matrix_store::const_ptr get_portion(
			size_t local_start_row, size_t local_start_col, size_t num_rows,
			size_t num_cols) const {
		assert(is_whole());
		store.materialize();
		return store.get_whole_res().get_portion(local_start_row,
				local_start_col, num_rows, num_cols);
	}
	virtual void materialize_self() const {
		if (!store.is_materialized())
			store.materialize();
	}
};

/*
 * This is the transpose of lmapply_col_matrix_store.
 */
class t_lmapply_col_matrix_store: public lvirtual_row_matrix_store
{
	lmapply_col_matrix_store::const_ptr store;
public:
	t_lmapply_col_matrix_store(
			lmapply_col_matrix_store::const_ptr store): lvirtual_row_matrix_store(
				store->get_global_start_col(), store->get_global_start_row(),
				store->get_num_cols(), store->get_num_rows(),
				store->get_type(), store->get_node_id()) {
		this->store = store;
	}

	virtual bool resize(off_t local_start_row, off_t local_start_col,
			size_t local_num_rows, size_t local_num_cols) {
		bool ret = const_cast<local_col_matrix_store &>(*store).resize(
				local_start_col, local_start_row, local_num_cols,
				local_num_rows);
		if (!ret)
			return false;
		return local_matrix_store::resize(local_start_row, local_start_col,
				local_num_rows, local_num_cols);
	}
	virtual void reset_size() {
		const_cast<local_col_matrix_store &>(*store).reset_size();
		local_matrix_store::reset_size();
	}

	using lvirtual_row_matrix_store::get_raw_arr;
	virtual const char *get_raw_arr() const {
		return store->get_raw_arr();
	}

	using lvirtual_row_matrix_store::transpose;
	virtual matrix_store::const_ptr transpose() const {
		return store;
	}

	using lvirtual_row_matrix_store::get_row;
	virtual const char *get_row(size_t row) const {
		return store->get_col(row);
	}

	virtual local_matrix_store::const_ptr get_portion(
			size_t local_start_row, size_t local_start_col, size_t num_rows,
			size_t num_cols) const {
		return store->get_portion(local_start_col, local_start_row, num_cols,
				num_rows);
	}
	virtual void materialize_self() const {
		store->materialize_self();
	}
};

/*
 * This is the transpose of lmapply_row_matrix_store.
 */
class t_lmapply_row_matrix_store: public lvirtual_col_matrix_store
{
	lmapply_row_matrix_store::const_ptr store;
public:
	t_lmapply_row_matrix_store(
			lmapply_row_matrix_store::const_ptr store): lvirtual_col_matrix_store(
				store->get_global_start_col(), store->get_global_start_row(),
				store->get_num_cols(), store->get_num_rows(),
				store->get_type(), store->get_node_id()) {
		this->store = store;
	}

	virtual bool resize(off_t local_start_row, off_t local_start_col,
			size_t local_num_rows, size_t local_num_cols) {
		bool ret = const_cast<local_row_matrix_store &>(*store).resize(
				local_start_col, local_start_row, local_num_cols,
				local_num_rows);
		if (!ret)
			return false;
		return local_matrix_store::resize(local_start_row, local_start_col,
				local_num_rows, local_num_cols);
	}
	virtual void reset_size() {
		const_cast<local_row_matrix_store &>(*store).reset_size();
		local_matrix_store::reset_size();
	}

	using lvirtual_col_matrix_store::get_raw_arr;
	virtual const char *get_raw_arr() const {
		return store->get_raw_arr();
	}

	using lvirtual_col_matrix_store::transpose;
	virtual matrix_store::const_ptr transpose() const {
		return store;
	}

	using lvirtual_col_matrix_store::get_col;
	virtual const char *get_col(size_t col) const {
		return store->get_row(col);
	}

	virtual local_matrix_store::const_ptr get_portion(
			size_t local_start_row, size_t local_start_col, size_t num_rows,
			size_t num_cols) const {
		return store->get_portion(local_start_col, local_start_row, num_cols,
				num_rows);
	}
	virtual void materialize_self() const {
		store->materialize_self();
	}
};

}

static inline bool is_all_in_mem(
		const std::vector<matrix_store::const_ptr> &in_mats)
{
	for (size_t i = 0; i < in_mats.size(); i++)
		if (!in_mats[i]->is_in_mem())
			return false;
	return true;
}

static inline int get_num_nodes(
		const std::vector<matrix_store::const_ptr> &mats)
{
	int num_nodes = -1;
	for (size_t i = 0; i < mats.size(); i++) {
		if (mats[i]->get_num_nodes() > 0) {
			if (num_nodes > 0)
				assert(mats[i]->get_num_nodes() == num_nodes);
			num_nodes = mats[i]->get_num_nodes();
		}
	}
	return num_nodes;
}

mapply_matrix_store::mapply_matrix_store(
		const std::vector<matrix_store::const_ptr> &_in_mats,
		portion_mapply_op::const_ptr op, matrix_layout_t layout,
		size_t _data_id): virtual_matrix_store(op->get_out_num_rows(),
			op->get_out_num_cols(), is_all_in_mem(_in_mats), op->get_output_type()),
		data_id(_data_id), in_mats(_in_mats)
{
	this->par_access = true;
	this->layout = layout;
	assert(layout == matrix_layout_t::L_ROW || layout == matrix_layout_t::L_COL);
	this->op = op;
	this->num_nodes = is_in_mem() ? fm::detail::get_num_nodes(in_mats) : -1;
	for (size_t i = 1; i < in_mats.size(); i++)
		assert(in_mats[0]->is_wide() == in_mats[i]->is_wide());
}

bool mapply_matrix_store::is_materialized() const
{
	return res != NULL && res->is_materialized();
}

void mapply_matrix_store::set_materialize_level(materialize_level level,
		detail::matrix_store::ptr materialize_buf)
{
	virtual_matrix_store::set_materialize_level(level, materialize_buf);
	if (level != materialize_level::MATER_FULL) {
		if (!is_materialized())
			res = NULL;
		return;
	}

	// If we need full materialization.
	if (res)
		return;

	if (materialize_buf == NULL) {
		size_t num_rows, num_cols;
		matrix_layout_t layout = store_layout();
		if (get_num_rows() < get_num_cols()) {
			num_cols = get_num_rows();
			num_rows = get_num_cols();
			if (layout == matrix_layout_t::L_ROW)
				layout = matrix_layout_t::L_COL;
			else
				layout = matrix_layout_t::L_ROW;
		}
		else {
			num_cols = get_num_cols();
			num_rows = get_num_rows();
		}
		matrix_store::ptr res_buf = matrix_store::create(num_rows, num_cols,
				layout, get_type(), get_num_nodes(), is_in_mem());
		assert(res_buf);
		res = materialized_mapply_tall_store::ptr(
				new materialized_mapply_tall_store(res_buf));
	}
	else {
		res = materialized_mapply_tall_store::ptr(
				new materialized_mapply_tall_store(materialize_buf));
	}
	const matrix_store &store = res->get_materialize_ref(store_layout());
	assert(store.get_num_rows() == get_num_rows());
	assert(store.get_num_cols() == get_num_cols());
}

void mapply_matrix_store::materialize_self() const
{
	// Materialize the matrix store if it hasn't.
	if (is_materialized())
		return;

	matrix_store::const_ptr materialized = __mapply_portion(in_mats, op,
			layout, is_in_mem(), get_num_nodes(), par_access);
	if (res)
		res->set_materialized(materialized);
	else {
		mapply_matrix_store *mutable_this
			= const_cast<mapply_matrix_store *>(this);
		mutable_this->res = materialized_mapply_tall_store::ptr(
				new materialized_mapply_tall_store(materialized));
	}
}

matrix_store::const_ptr mapply_matrix_store::materialize(bool in_mem,
			int num_nodes) const
{
	if (is_materialized())
		// The input arguments only provide some guidance for where
		// the materialized data should be stored. If the matrix has been
		// materialized, we don't need to move the data.
		return this->res->get_materialize_res(store_layout());
	else
		return __mapply_portion(in_mats, op, layout, in_mem, num_nodes,
				par_access);
}

matrix_store::const_ptr mapply_matrix_store::get_rows(
		const std::vector<off_t> &idxs) const
{
	if (is_materialized())
		return res->get_materialize_res(store_layout())->get_rows(idxs);
	if (is_wide()) {
		// We rely on get_rows in dense_matrix to materialize the matrix
		// and get the required rows on the fly.
		// TODO maybe we should optimize it and materialize part of it.
		// It's possible if the computation is element-wise.
		return matrix_store::const_ptr();
	}

	// In this case, we are dealing with a tall matrix and need to find out
	// the portions in the matrix that need to be materialized.
	mem_matrix_store::ptr ret = mem_matrix_store::create(idxs.size(),
			get_num_cols(), matrix_layout_t::L_ROW, get_type(), -1);
	std::unordered_map<off_t, local_matrix_store::const_ptr> portions;
	size_t portion_size = get_portion_size().first;
	std::vector<const char *> src_eles(get_num_cols());
	for (size_t i = 0; i < idxs.size(); i++) {
		off_t portion_idx = idxs[i] / portion_size;
		auto it = portions.find(portion_idx);
		local_matrix_store::const_ptr portion;
		if (it == portions.end()) {
			// TODO it might be better if we get portions asynchronously.
			// For in-memory matrix, this approach may waste a lot of computation
			// because we have to construct the entire portion, which is
			// unnecessary in some cases.
			portion = get_portion(portion_idx);
			portions.insert(std::pair<off_t, local_matrix_store::const_ptr>(
						portion_idx, portion));
		}
		else
			portion = it->second;

		if (portion->store_layout() == matrix_layout_t::L_ROW) {
			local_row_matrix_store::const_ptr row_portion
				= std::static_pointer_cast<const local_row_matrix_store>(portion);
			memcpy(ret->get_row(i),
					row_portion->get_row(idxs[i] - portion->get_global_start_row()),
					get_num_cols() * get_type().get_size());
		}
		else {
			local_col_matrix_store::const_ptr col_portion
				= std::static_pointer_cast<const local_col_matrix_store>(portion);
			for (size_t j = 0; j < get_num_cols(); j++)
				src_eles[j] = portion->get(idxs[i] - portion->get_global_start_row(), j);
			get_type().get_sg().gather(src_eles, ret->get_row(i));
		}
	}
	return ret;
}

static local_matrix_store::const_ptr transpose_lmapply(
		local_matrix_store::const_ptr store)
{
	if (store->store_layout() == matrix_layout_t::L_COL) {
		lmapply_col_matrix_store::const_ptr store1
			= std::static_pointer_cast<const lmapply_col_matrix_store>(store);
		return local_matrix_store::const_ptr(
				new t_lmapply_col_matrix_store(store1));
	}
	else {
		lmapply_row_matrix_store::const_ptr store1
			= std::static_pointer_cast<const lmapply_row_matrix_store>(store);
		return local_matrix_store::const_ptr(
				new t_lmapply_row_matrix_store(store1));
	}
}

local_matrix_store::const_ptr mapply_matrix_store::get_portion(
			size_t start_row, size_t start_col, size_t num_rows,
			size_t num_cols) const
{
	// We should try to get the portion from the local thread memory buffer
	// first.
	local_matrix_store::const_ptr ret = local_mem_buffer::get_mat_portion(
			data_id);
	bool same_portion = false;
	bool trans_portion = false;
	if (ret) {
		// If it's in the same portion.
		same_portion = (size_t) ret->get_global_start_row() == start_row
			&& (size_t) ret->get_global_start_col() == start_col
			&& ret->get_num_rows() == num_rows
			&& ret->get_num_cols() == num_cols
			&& ret->store_layout() == store_layout();
		// If it's in the corresponding portion in the transposed matrix.
		trans_portion = (size_t) ret->get_global_start_row() == start_col
			&& (size_t) ret->get_global_start_col() == start_row
			&& ret->get_num_rows() == num_cols
			&& ret->get_num_cols() == num_rows
			&& ret->store_layout() != store_layout();
	}
	if (same_portion || trans_portion) {
		assert(ret->get_local_start_row() == 0);
		assert(ret->get_local_start_col() == 0);
		return trans_portion ? transpose_lmapply(ret) : ret;
	}

	// If the virtual matrix store has been materialized, we should return
	// the portion from the materialized store directly.
	// If the materialized matrix store is external memory, it should cache
	// the portion itself.
	if (is_materialized())
		return res->get_materialize_ref(store_layout()).get_portion(
				start_row, start_col, num_rows, num_cols);

	std::vector<local_matrix_store::const_ptr> parts(in_mats.size());
	if (is_wide()) {
		assert(start_row == 0);
		assert(num_rows == get_num_rows());
		for (size_t i = 0; i < in_mats.size(); i++)
			parts[i] = in_mats[i]->get_portion(start_row, start_col,
					in_mats[i]->get_num_rows(), num_cols);
	}
	else {
		assert(start_col == 0);
		assert(num_cols == get_num_cols());
		for (size_t i = 0; i < in_mats.size(); i++)
			parts[i] = in_mats[i]->get_portion(start_row, start_col,
					num_rows, in_mats[i]->get_num_cols());
	}

	if (store_layout() == matrix_layout_t::L_ROW)
		ret = local_matrix_store::const_ptr(new lmapply_row_matrix_store(
					get_materialize_level(), parts, *op, res.get(), NULL,
					start_row, start_col, num_rows, num_cols, get_type(),
					parts.front()->get_node_id()));
	else
		ret = local_matrix_store::const_ptr(new lmapply_col_matrix_store(
					get_materialize_level(), parts, *op, res.get(), NULL,
					start_row, start_col, num_rows, num_cols, get_type(),
					parts.front()->get_node_id()));
	if (is_cache_portion())
		local_mem_buffer::cache_portion(data_id, ret);
	return ret;
}

local_matrix_store::const_ptr mapply_matrix_store::get_portion(
			size_t id) const
{
	size_t start_row;
	size_t start_col;
	size_t num_rows;
	size_t num_cols;
	std::pair<size_t, size_t> chunk_size = get_portion_size();
	if (is_wide()) {
		start_row = 0;
		start_col = chunk_size.second * id;
		num_rows = get_num_rows();
		num_cols = std::min(chunk_size.second, get_num_cols() - start_col);
	}
	else {
		start_row = chunk_size.first * id;
		start_col = 0;
		num_rows = std::min(chunk_size.first, get_num_rows() - start_row);
		num_cols = get_num_cols();
	}
	return get_portion(start_row, start_col, num_rows, num_cols);
}

int mapply_matrix_store::get_portion_node_id(size_t id) const
{
	int node_id = -1;
	for (size_t i = 0; i < in_mats.size(); i++) {
		if (node_id < 0)
			node_id = in_mats[i]->get_portion_node_id(id);
		// We assume that all portions are either in the same node
		// or aren't associated with a node.
		// It might be expensive to compute if mapply matrix has
		// a deep hierarchy, so depth-first search can reduce the cost.
		if (node_id >= 0)
			break;
	}
	return node_id;
}

async_cres_t mapply_matrix_store::get_portion_async(
		size_t start_row, size_t start_col, size_t num_rows,
		size_t num_cols, portion_compute::ptr orig_compute) const
{
	// If the virtual matrix store has been materialized, we should return
	// the portion from the materialized store directly.
	// If the materialized matrix store is external memory, it should cache
	// the portion itself.
	if (is_materialized())
		return res->get_materialize_ref(store_layout()).get_portion_async(
				start_row, start_col, num_rows, num_cols, orig_compute);

	// We should try to get the portion from the local thread memory buffer
	// first.
	local_matrix_store::const_ptr ret1 = local_mem_buffer::get_mat_portion(
			data_id);
	// If it's in the same portion.
	if (ret1 && (((size_t) ret1->get_global_start_row() == start_row
					&& (size_t) ret1->get_global_start_col() == start_col
					&& ret1->get_num_rows() == num_rows
					&& ret1->get_num_cols() == num_cols)
				// If it's in the corresponding portion in the transposed matrix.
				|| ((size_t) ret1->get_global_start_row() == start_col
					&& (size_t) ret1->get_global_start_col() == start_row
					&& ret1->get_num_rows() == num_cols
					&& ret1->get_num_cols() == num_rows))) {
		assert(ret1->get_local_start_row() == 0);
		assert(ret1->get_local_start_col() == 0);
		// In the asynchronous version, data in the portion isn't ready when
		// the method is called. We should add the user's portion computation
		// to the queue. When the data is ready, all user's portion computations
		// will be invoked.
		local_matrix_store *tmp = const_cast<local_matrix_store *>(ret1.get());
		collect_portion_compute::ptr collect_compute;
		if (ret1->store_layout() == matrix_layout_t::L_COL) {
			lmapply_col_matrix_store *store
				= dynamic_cast<lmapply_col_matrix_store *>(tmp);
			assert(store);
			collect_compute = store->get_compute();
		}
		else {
			lmapply_row_matrix_store *store
				= dynamic_cast<lmapply_row_matrix_store *>(tmp);
			assert(store);
			collect_compute = store->get_compute();
		}
		// If the cached portion has different data layout from the matrix,
		// the cached portion is transposed. We need to transpose the matrix.
		if (ret1->store_layout() != store_layout())
			ret1 = transpose_lmapply(ret1);
		// If the collect compute doesn't exist, it mean the data in the local
		// matrix store may already by ready.
		if (collect_compute) {
			collect_compute->add_orig_compute(orig_compute);
			return async_cres_t(false, ret1);
		}
		else
			return async_cres_t(true, ret1);
	}

	collect_portion_compute::ptr collect_compute;
	if (get_materialize_level() == materialize_level::MATER_FULL) {
		assert(res);
		collect_compute = collect_portion_compute::ptr(
				new collect_portion_compute(orig_compute, res, is_wide()));
	}
	else
		collect_compute = collect_portion_compute::ptr(
				new collect_portion_compute(orig_compute, NULL, is_wide()));

	std::vector<local_matrix_store::const_ptr> parts(in_mats.size());
	if (is_wide()) {
		assert(start_row == 0);
		assert(num_rows == get_num_rows());
		for (size_t i = 0; i < in_mats.size(); i++) {
			async_cres_t res = in_mats[i]->get_portion_async(start_row, start_col,
					in_mats[i]->get_num_rows(), num_cols, collect_compute);
			parts[i] = res.second;
			// If the data in the request portion is invalid.
			if (!res.first && collect_compute)
				collect_compute->add_EM_part(parts[i]);
		}
	}
	else {
		assert(start_col == 0);
		assert(num_cols == get_num_cols());
		for (size_t i = 0; i < in_mats.size(); i++) {
			async_cres_t res = in_mats[i]->get_portion_async(start_row, start_col,
					num_rows, in_mats[i]->get_num_cols(), collect_compute);
			parts[i] = res.second;
			// If the data in the request portion is invalid.
			if (!res.first && collect_compute)
				collect_compute->add_EM_part(parts[i]);
		}
	}

	local_matrix_store::ptr ret;
	if (store_layout() == matrix_layout_t::L_ROW)
		ret = local_matrix_store::ptr(new lmapply_row_matrix_store(
					get_materialize_level(), parts, *op, res.get(),
					collect_compute, start_row, start_col, num_rows,
					num_cols, get_type(), parts.front()->get_node_id()));
	else
		ret = local_matrix_store::ptr(new lmapply_col_matrix_store(
					get_materialize_level(), parts, *op, res.get(),
					collect_compute, start_row, start_col, num_rows,
					num_cols, get_type(), parts.front()->get_node_id()));
	if (collect_compute)
		collect_compute->set_res_part(ret);
	if (is_cache_portion())
		local_mem_buffer::cache_portion(data_id, ret);
	// If all parts are from the in-mem matrix store or have been cached by
	// the underlying matrices, the data in the returned portion is immediately
	// accessible.
	return async_cres_t(collect_compute->get_num_EM_parts() == 0, ret);
}

std::pair<size_t, size_t> mapply_matrix_store::get_portion_size() const
{
	size_t size = is_in_mem()
		? mem_matrix_store::CHUNK_SIZE : EM_matrix_store::CHUNK_SIZE;
	// I should use a relatively small chunk size here. Otherwise,
	// the aggregated memory size for buffering a portion of each matrix
	// will be too large.
	if (is_wide())
		return std::pair<size_t, size_t>(get_num_rows(), size);
	else
		return std::pair<size_t, size_t>(size, get_num_cols());
}

matrix_store::const_ptr mapply_matrix_store::transpose() const
{
	std::vector<matrix_store::const_ptr> t_in_mats(in_mats.size());
	for (size_t i = 0; i < in_mats.size(); i++)
		t_in_mats[i] = in_mats[i]->transpose();
	matrix_layout_t t_layout;
	if (layout == matrix_layout_t::L_COL)
		t_layout = matrix_layout_t::L_ROW;
	else
		t_layout = matrix_layout_t::L_COL;
	mapply_matrix_store *ret = new mapply_matrix_store(t_in_mats,
			op->transpose(), t_layout, data_id);
	ret->res = this->res;
	// The transposed matrix should share the same materialization level.
	ret->set_materialize_level(get_materialize_level(), NULL);
	return matrix_store::const_ptr(ret);
}

std::vector<safs::io_interface::ptr> mapply_matrix_store::create_ios() const
{
	// If the matrix has been materialized and it's stored on disks,
	if (is_materialized() && !res->is_in_mem()) {
		const EM_object *obj = dynamic_cast<const EM_object *>(
				res->get_materialize_res(store_layout()).get());
		assert(obj);
		return obj->create_ios();
	}

	// Otherwise, we get IO instances from all EM matrices in the virtual matrix.
	std::vector<safs::io_interface::ptr> ret;
	for (size_t i = 0; i < in_mats.size(); i++) {
		if (!in_mats[i]->is_in_mem()) {
			const EM_object *obj
				= dynamic_cast<const EM_object *>(in_mats[i].get());
			std::vector<safs::io_interface::ptr> tmp = obj->create_ios();
			ret.insert(ret.end(), tmp.begin(), tmp.end());
		}
	}
	if (res && !res->is_materialized() && !res->is_in_mem()) {
		const EM_object *obj = dynamic_cast<const EM_object *>(
				res->get_materialize_res(store_layout()).get());
		std::vector<safs::io_interface::ptr> tmp = obj->create_ios();
		ret.insert(ret.end(), tmp.begin(), tmp.end());
	}
	return ret;
}

std::string mapply_matrix_store::get_name() const
{
	if (is_materialized())
		return this->res->get_materialize_res(store_layout())->get_name();
	else
		return (boost::format("vmat-%1%(%2%,%3%,%4%)=") % data_id
				% get_num_rows() % get_num_cols()
				% (store_layout() == matrix_layout_t::L_ROW ? "row" : "col")).str()
			+ op->to_string(in_mats);
}

std::unordered_map<size_t, size_t> mapply_matrix_store::get_underlying_mats() const
{
	if (is_materialized())
		return res->get_materialize_ref(store_layout()).get_underlying_mats();

	std::unordered_map<size_t, size_t> final_res;
	for (size_t i = 0; i < in_mats.size(); i++) {
		std::unordered_map<size_t, size_t> res = in_mats[i]->get_underlying_mats();
		/*
		 * When we merge, we only need to collect the matrices with different
		 * data IDs.
		 */
		for (auto it = res.begin(); it != res.end(); it++) {
			auto to_it = final_res.find(it->first);
			if (to_it == final_res.end())
				final_res.insert(std::pair<size_t, size_t>(it->first, it->second));
		}
	}
	return final_res;
}

void mapply_matrix_store::set_prefetches(size_t num,
		std::pair<size_t, size_t> range)
{
	for (size_t i = 0; i < in_mats.size(); i++)
		const_cast<matrix_store &>(*in_mats[i]).set_prefetches(num, range);
}

void mapply_matrix_store::set_cache_portion(bool cache_portion)
{
	matrix_store::set_cache_portion(cache_portion);
	for (size_t i = 0; i < in_mats.size(); i++)
		const_cast<matrix_store &>(*in_mats[i]).set_cache_portion(cache_portion);
}

}

}
