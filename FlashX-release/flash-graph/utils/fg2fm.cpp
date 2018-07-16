/*
 * Copyright 2014 Open Connectome Project (http://openconnecto.me)
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
#include "common.h"

#include "safs_file.h"
#include "io_interface.h"

#include "utils.h"
#include "fg_utils.h"

#include "vertex_index.h"

#include "vector_vector.h"
#include "fm_utils.h"
#include "fg_utils.h"
#include "EM_vector.h"
#include "mem_vec_store.h"
#include "EM_vv_store.h"
#include "mem_vv_store.h"
#include "sparse_matrix.h"

using namespace fm;

void verify_2d_matrix(const std::string &mat_file,
		const std::string &mat_idx_file, bool from_safs)
{
	SpM_2d_index::ptr idx;
	SpM_2d_storage::ptr mat;
	if (from_safs)
		idx = SpM_2d_index::safs_load(mat_idx_file);
	else
		idx = SpM_2d_index::load(mat_idx_file);
	if (from_safs)
		mat = SpM_2d_storage::safs_load(mat_file, idx);
	else
		mat = SpM_2d_storage::load(mat_file, idx);
	mat->verify();
}

void read_data(char *data, size_t size, size_t num, off_t off, FILE *stream)
{
	int iret = fseek(stream, off, SEEK_SET);
	assert(iret == 0);
	size_t ret = fread(data, size, num, stream);
	assert(ret == num);
}

fg::vertex_index::ptr load_vertex_index(const std::string &index_file)
{
	fg::vertex_index::ptr vindex;
	if (safs::exist_safs_file(index_file))
		vindex = fg::vertex_index::safs_load(index_file);
	else {
		safs::native_file f(index_file);
		if (!f.exist()) {
			fprintf(stderr,
					"The index file %s doesn't exist in either Linux filesystem or SAFS\n",
					index_file.c_str());
			return fg::vertex_index::ptr();
		}
		vindex = fg::vertex_index::load(index_file);
	}
	return vindex;
}

vector_vector::ptr load_graph(const std::string &graph_file,
		fg::vertex_index::ptr vindex, bool is_out_edge)
{
	std::vector<off_t> offs(vindex->get_num_vertices() + 1);
	printf("find the location of adjacency lists\n");
	if (is_out_edge)
		fg::init_out_offs(vindex, offs);
	else
		fg::init_in_offs(vindex, offs);

	size_t size;
	off_t off;
	size = offs.back() - offs.front();
	off = offs.front();

	safs::native_file f(graph_file);
	detail::vv_store::ptr vv;
	if (f.exist()) {
		FILE *f = fopen(graph_file.c_str(), "r");
		if (f == NULL) {
			fprintf(stderr, "can't open %s: %s\n", graph_file.c_str(),
					strerror(errno));
			return vector_vector::ptr();
		}
		detail::simple_raw_array data(size, -1);
		printf("load adjacency lists of the graph data.\n");
		read_data(data.get_raw(), size, 1, off, f);
		fclose(f);

		for (size_t i = 0; i < offs.size(); i++)
			offs[i] -= off;
		vv = detail::mem_vv_store::create(data, offs, get_scalar_type<char>());
	}
	else {
		if (!safs::exist_safs_file(graph_file)) {
			fprintf(stderr, "The graph file %s doesn't exist in SAFS\n",
					graph_file.c_str());
			return vector_vector::ptr();
		}
		safs::file_io_factory::shared_ptr factory = safs::create_io_factory(
				graph_file, safs::REMOTE_ACCESS);
		if (factory == NULL) {
			fprintf(stderr, "can't access the SAFS file %s\n",
					graph_file.c_str());
			return vector_vector::ptr();
		}

		detail::EM_object::file_holder::ptr holder
			= detail::EM_object::file_holder::create(factory->get_name());
		detail::EM_object::io_set::ptr ios(new detail::EM_object::io_set(factory));
		size_t file_size = factory->get_file_size();
		detail::EM_vec_store::ptr vec = detail::EM_vec_store::create(holder, ios,
				file_size, get_scalar_type<char>());
		vv = detail::EM_vv_store::create(offs, vec);
	}

	return vector_vector::create(vv);
}

void print_usage()
{
	fprintf(stderr,
			"convert the adjacency list of FlashGraph to 2D-partitioned matrix\n");
	fprintf(stderr, "al22d [options] conf_file graph_file index_file matrix_name\n");
	fprintf(stderr, "-h height: the height of a 2D-partitioned matrix block\n");
	fprintf(stderr, "-w width: the width of a 2D-partitioned matrix block\n");
	fprintf(stderr, "-v: verify the generated matrix image\n");
	fprintf(stderr, "-g size: the groupby buffer size on vector vectors\n");
	fprintf(stderr, "-t type: the type of non-zero entries\n");
}

int main(int argc, char *argv[])
{
	size_t block_height = block_max_num_rows;
	size_t block_width = block_height;
	size_t vv_groupby_buf_size = 1024 * 1024;
	bool verify = false;
	int opt;
	int num_opts = 0;
	std::string entry_type;
	while ((opt = getopt(argc, argv, "h:w:vg:t:")) != -1) {
		num_opts++;
		switch (opt) {
			case 'w':
				block_width = str2size(optarg);
				num_opts++;
				break;
			case 'h':
				block_height = str2size(optarg);
				num_opts++;
				break;
			case 'v':
				verify = true;
				break;
			case 'g':
				vv_groupby_buf_size = str2size(optarg);
				num_opts++;
				break;
			case 't':
				entry_type = optarg;
				num_opts++;
				break;
			default:
				print_usage();
				exit(1);
		}
	}
	block_2d_size block_size(block_height, block_width);

	argv += 1 + num_opts;
	argc -= 1 + num_opts;
	if (argc < 4) {
		print_usage();
		exit(1);
	}

	std::string conf_file = argv[0];
	std::string graph_file = argv[1];
	std::string index_file = argv[2];
	std::string mat_name = argv[3];
	std::string mat_file = mat_name + ".mat";
	std::string mat_idx_file = mat_name + ".mat_idx";

	config_map::ptr configs = config_map::create(conf_file);
	matrix_conf.set_vv_groupby_buf_size(vv_groupby_buf_size);
	init_flash_matrix(configs);

	printf("load vertex index\n");
	fg::vertex_index::ptr vindex = load_vertex_index(index_file);
	printf("The graph has %ld vertices and %ld edges\n",
			vindex->get_num_vertices(), vindex->get_graph_header().get_num_edges());

	const scalar_type *type = NULL;
	if (entry_type.empty())
		type = NULL;
	else if (entry_type == "I")
		type = &get_scalar_type<int>();
	else if (entry_type == "L")
		type = &get_scalar_type<long>();
	else if (entry_type == "F")
		type = &get_scalar_type<float>();
	else if (entry_type == "D")
		type = &get_scalar_type<double>();
	else {
		fprintf(stderr, "unsupported entry type\n");
		exit(1);
	}

	if (type)
		assert(type->get_size()
				== (size_t) vindex->get_graph_header().get_edge_data_size());
	vector_vector::ptr out_adjs = load_graph(graph_file, vindex, true);
	bool to_safs = !out_adjs->is_in_mem();
	// Construct 2D partitioning of the adjacency matrix.
	printf("export 2d matrix for the out-adjacency lists\n");
	fg::export_2d_matrix(out_adjs, out_adjs->get_num_vecs(), block_size, type,
			mat_file, mat_idx_file, to_safs);
	if (verify) {
		printf("verify 2d matrix for the out-adjacency lists\n");
		verify_2d_matrix(mat_file, mat_idx_file, to_safs);
	}
	out_adjs = NULL;

	if (vindex->get_graph_header().is_directed_graph()) {
		std::string t_mat_file = mat_name + "_t.mat";
		std::string t_mat_idx_file = mat_name + "_t.mat_idx";

		vector_vector::ptr in_adjs = load_graph(graph_file, vindex, false);
		// Construct 2D partitioning of the adjacency matrix.
		printf("export 2d matrix for the in-adjacency lists\n");
		fg::export_2d_matrix(in_adjs, in_adjs->get_num_vecs(), block_size, type,
				t_mat_file, t_mat_idx_file, to_safs);
		if (verify) {
			printf("verify 2d matrix for the in-adjacency lists\n");
			verify_2d_matrix(t_mat_file, t_mat_idx_file, to_safs);
		}
	}

	destroy_flash_matrix();
}
