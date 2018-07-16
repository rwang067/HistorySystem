#include <malloc.h>

#include "safs_file.h"

#include "EM_vector.h"
#include "sparse_matrix.h"
#include "matrix_config.h"
#include "mem_worker_thread.h"

using namespace fm;
using namespace detail;

class write_double_compute: public portion_compute
{
	double *buf;
public:
	write_double_compute(double *buf) {
		this->buf = buf;
	}

	~write_double_compute() {
		free(buf);
	}

	virtual void run(char *buf, size_t size) {
	}
};

class read_double_compute: public portion_compute
{
	double *buf;
	off_t idx;
	size_t num;
public:
	read_double_compute(double *buf, off_t idx, size_t num) {
		this->buf = buf;
		this->idx = idx;
		this->num = num;
	}

	virtual void run(char *buf, size_t size) {
		double *dbuf = (double *) buf;
		assert(sizeof(double) * num == size);
		for (size_t i = 0; i < num; i++)
			assert(dbuf[i] == idx + i);
		free(this->buf);
	}
};

template<class T>
class set_seq_operate: public set_vec_operate
{
public:
	virtual void set(void *tmp, size_t num_eles, off_t start_idx) const {
		T *arr = (T *) tmp;
		for (size_t i = 0; i < num_eles; i++)
			arr[i] = start_idx + i;
	}
	virtual const scalar_type &get_type() const {
		return get_scalar_type<T>();
	}
};

template<class T>
class set_rand_operate: public set_vec_operate
{
	T max_val;
public:
	set_rand_operate(T max_val) {
		this->max_val = max_val;
	}

	virtual void set(void *tmp, size_t num_eles, off_t start_idx) const {
		T *arr = (T *) tmp;
		for (size_t i = 0; i < num_eles; i++)
			arr[i] = random() % max_val;
	}
	virtual const scalar_type &get_type() const {
		return get_scalar_type<T>();
	}
};

void test_setdata()
{
	printf("test set data in EM vector\n");
	EM_vec_store::ptr vec = EM_vec_store::create(10000000 + random() % 1000000,
			get_scalar_type<int>());
	vec->set_data(set_seq_operate<int>());
	assert(vec->is_sorted());
}

void test_sort_summary()
{
	printf("test sort summary\n");
	const scalar_type &type = get_scalar_type<int>();
	size_t num_bufs = 10;
	std::pair<size_t, size_t> sizes = EM_sort_detail::cal_sort_buf_size(type,
			matrix_conf.get_sort_buf_size() / type.get_size() * num_bufs);
	size_t sort_buf_size = sizes.first;
	size_t anchor_gap_size = sizes.second;
	std::vector<local_buf_vec_store::ptr> bufs(num_bufs);
	EM_sort_detail::sort_portion_summary summary(num_bufs, sort_buf_size,
			anchor_gap_size);
	for (size_t i = 0; i < num_bufs; i++) {
		local_buf_vec_store::ptr buf(new local_buf_vec_store(i * sort_buf_size,
					sort_buf_size, type, -1));
		buf->set_data(set_rand_operate<int>(1000));
		buf->sort();
		summary.add_portion(buf);
	}

	std::vector<int> anchor_vals;
	std::vector<local_vec_store::const_ptr> anchor_bufs(num_bufs);
	assert(summary.get_num_bufs() == num_bufs);
	for (size_t i = 0; i < num_bufs; i++) {
		local_vec_store::const_ptr buf = summary.get_anchor_vals(i);
		assert(buf->is_sorted());
		anchor_bufs[i] = buf;

		int *start = (int *) buf->get_raw_arr();
		anchor_vals.insert(anchor_vals.end(), start, start + buf->get_length());
	}
	std::sort(anchor_vals.begin(), anchor_vals.end());

	EM_sort_detail::anchor_prio_queue::ptr queue = summary.get_prio_queue();
	size_t max_fetch_size = anchor_gap_size * 8;
	size_t fetch_size = random() % max_fetch_size;
	size_t min_loc = 0;
	std::vector<off_t> anchor_locs = queue->pop(fetch_size);
	while (!anchor_locs.empty()) {
		scalar_variable::ptr min_val = queue->get_min_frontier();
		if (min_val == NULL)
			break;

		assert(anchor_locs.size() * anchor_gap_size == ROUNDUP(fetch_size,
					anchor_gap_size));
		min_loc += anchor_locs.size();
		assert(*(int *) min_val->get_raw() == anchor_vals[min_loc]);

		fetch_size = random() % max_fetch_size;
		anchor_locs = queue->pop(fetch_size);
	}
}

void test_sort()
{
	printf("test sort\n");
	EM_vec_store::ptr vec = EM_vec_store::create(10000000 + random() % 1000000,
			get_scalar_type<int>());
	vec->set_data(set_rand_operate<int>(1000));
	local_vec_store::ptr copy = vec->get_portion(0, vec->get_length());

	assert(!vec->is_sorted());
	printf("sort vec\n");
	vec->sort();
	assert(vec->is_sorted());
	local_vec_store::ptr sorted_copy = vec->get_portion(0, vec->get_length());
	copy->get_type().get_sorter().sort(copy->get_raw_arr(), copy->get_length(),
			false);
	assert(memcmp(copy->get_raw_arr(), sorted_copy->get_raw_arr(),
				copy->get_length() * copy->get_type().get_size()) == 0);
}

void test_sort_mult1()
{
	printf("test sort a single vector\n");
	EM_vec_store::ptr vec = EM_vec_store::create(10000000 + random() % 1000000,
			get_scalar_type<int>());
	vec->set_data(set_rand_operate<int>(1000));
	local_vec_store::ptr copy = vec->get_portion(0, vec->get_length());

	std::vector<EM_vec_store::const_ptr> vecs(1);
	vecs[0] = vec;
	assert(!vec->is_sorted());
	printf("sort vec\n");
	std::vector<EM_vec_store::ptr> sorted_vecs = sort(vecs);
	assert(sorted_vecs[0]->is_sorted());

	local_vec_store::ptr sorted_copy = sorted_vecs[0]->get_portion(0, vec->get_length());
	copy->get_type().get_sorter().sort(copy->get_raw_arr(), copy->get_length(),
			false);
	assert(memcmp(copy->get_raw_arr(), sorted_copy->get_raw_arr(),
				copy->get_length() * copy->get_type().get_size()) == 0);
}

void test_sort_mult()
{
	printf("test sort multiple vectors\n");
	EM_vec_store::ptr vec1 = EM_vec_store::create(10000000 + random() % 1000000,
			get_scalar_type<int>());
	vec1->set_data(set_rand_operate<int>(1000));
	EM_vec_store::ptr vec2 = EM_vec_store::cast(vec1->deep_copy());

	std::vector<EM_vec_store::const_ptr> vecs(2);
	vecs[0] = vec1;
	vecs[1] = vec2;
	assert(!vec1->is_sorted());
	assert(!vec2->is_sorted());
	printf("sort vec\n");
	std::vector<EM_vec_store::ptr> sorted_vecs = sort(vecs);
	assert(sorted_vecs[0]->is_sorted());
	assert(sorted_vecs[1]->is_sorted());

	local_vec_store::ptr copy1 = sorted_vecs[0]->get_portion(0,
			sorted_vecs[0]->get_length());
	local_vec_store::ptr copy2 = sorted_vecs[1]->get_portion(0,
			sorted_vecs[1]->get_length());
	assert(memcmp(copy1->get_raw_arr(), copy2->get_raw_arr(),
			copy1->get_length() * copy1->get_type().get_size()) == 0);
}

void test_append()
{
	printf("test append in-mem vectors\n");
	std::vector<vec_store::const_ptr> vecs(5);
	for (size_t i = 0; i < vecs.size(); i++) {
		vec_store::ptr tmp = smp_vec_store::create(1000000, get_scalar_type<int>());
		tmp->set_data(set_seq_operate<int>());
		vecs[i] = tmp;
	}
	EM_vec_store::ptr em_vec = EM_vec_store::create(0, get_scalar_type<int>());
	em_vec->append(vecs.begin(), vecs.end());

	vector::ptr vec1 = vector::create(em_vec);
	dense_matrix::ptr mat1 = vec1->conv2mat(vec1->get_length(), 1, false);
	mat1 = mat1->cast_ele_type(get_scalar_type<long>());
	scalar_variable::ptr sum1 = mat1->sum();
	printf("%ld\n", scalar_variable::get_val<long>(*sum1));

	vec_store::ptr tmp_vec = em_vec->deep_copy();
	vector::ptr vec2 = vector::create(tmp_vec);
	dense_matrix::ptr mat2 = vec2->conv2mat(vec2->get_length(), 1, false);
	mat2 = mat2->cast_ele_type(get_scalar_type<long>());
	scalar_variable::ptr sum2 = mat2->sum();
	assert(scalar_variable::get_val<long>(*sum2)
			== scalar_variable::get_val<long>(*sum1));

	tmp_vec->append(*em_vec);
	printf("tmp len: %ld, em len: %ld\n", tmp_vec->get_length(), em_vec->get_length());
	vector::ptr vec3 = vector::create(tmp_vec);
	dense_matrix::ptr mat3 = vec3->conv2mat(vec3->get_length(), 1, false);
	mat3 = mat3->cast_ele_type(get_scalar_type<long>());
	scalar_variable::ptr sum3 = mat3->sum();
	printf("sum3: %ld, sum1: %ld\n", scalar_variable::get_val<long>(*sum3),
			scalar_variable::get_val<long>(*sum1));
	assert(scalar_variable::get_val<long>(*sum3)
			== 2 * scalar_variable::get_val<long>(*sum1));
}

void test_set_persistent()
{
	printf("test set persistent\n");
	EM_vec_store::ptr vec = EM_vec_store::create(1000000, get_scalar_type<int>());
	std::string file_name = "test.vec";
	bool ret = vec->set_persistent(file_name);
	assert(ret);
	vec = NULL;
	safs::safs_file f(safs::get_sys_RAID_conf(), file_name);
	assert(f.exist());
	f.delete_file();
}

int main(int argc, char *argv[])
{
	if (argc < 2) {
		fprintf(stderr, "test conf_file\n");
		exit(1);
	}

	std::string conf_file = argv[1];
	config_map::ptr configs = config_map::create(conf_file);
	init_flash_matrix(configs);
	matrix_conf.set_sort_buf_size(1024 * 1024 * 8);
	matrix_conf.set_write_io_buf_size(1024 * 1024);

	test_append();
	test_set_persistent();
	test_setdata();
	test_sort_summary();
	test_sort();
	test_sort_mult();
	test_sort_mult1();

	destroy_flash_matrix();
}
