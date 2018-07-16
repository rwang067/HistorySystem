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

#include "vector_vector.h"
#include "mem_vv_store.h"
#include "local_vv_store.h"
#include "local_vec_store.h"

namespace fm
{

namespace detail
{

mem_vv_store::ptr mem_vv_store::cast(vec_store::ptr store)
{
	if (!store->is_in_mem() || store->get_entry_size() != 0) {
		BOOST_LOG_TRIVIAL(error) << "This isn't mem vv store\n";
		return mem_vv_store::ptr();
	}
	return std::static_pointer_cast<mem_vv_store>(store);
}

void mem_vv_store::set_data(const set_vv_operate &op)
{
#pragma omp parallel for
	for (size_t i = 0; i < get_num_vecs(); i++)
		op.set(i, get_raw_arr(i), get_length(i));
}

}

}
