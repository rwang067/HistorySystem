#include <signal.h>

#include <boost/format.hpp>

#include "matrix_config.h"
#include "sparse_matrix.h"
#include "dense_matrix.h"

using namespace fm;

size_t length = 4096L * 1024 * 1024;

template<class T>
class mat_init: public type_set_operate<T>
{
public:
	virtual void set(T *arr, size_t num_eles, off_t row_idx,
			            off_t col_idx) const {
		for (size_t i = 0; i < num_eles; i++)
			arr[i] = i;
	}
	virtual set_operate::const_ptr transpose() const {
		return set_operate::const_ptr();
	}
};

void test_mapply()
{
	struct timeval start, end;
	typedef size_t ele_type;
	size_t height = 1024L * 1024 * 1024;
	size_t width = 8;

	dense_matrix::ptr mat1 = dense_matrix::create(height, width,
			matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	dense_matrix::ptr mat2 = dense_matrix::create(height, width,
			matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	bulk_operate::const_ptr min
		= bulk_operate::conv2ptr(*mat1->get_type().get_basic_ops().get_op(basic_ops::op_idx::MIN));
	bulk_operate::const_ptr max
		= bulk_operate::conv2ptr(*mat1->get_type().get_basic_ops().get_op(basic_ops::op_idx::MAX));
	bulk_operate::const_ptr add
		= bulk_operate::conv2ptr(*mat1->get_type().get_basic_ops().get_op(basic_ops::op_idx::ADD));
	bulk_operate::const_ptr sub
		= bulk_operate::conv2ptr(*mat1->get_type().get_basic_ops().get_op(basic_ops::op_idx::SUB));

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res1 = mat1->mapply2(*mat2, min);
		res1->materialize_self();
		dense_matrix::ptr res2 = mat1->mapply2(*mat2, max);
		res2->materialize_self();
		gettimeofday(&end, NULL);
		printf("separate min+max takes %.3f seconds\n", time_diff(start, end));
	}

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res1 = mat1->mapply2(*mat2, min);
		dense_matrix::ptr res2 = mat1->mapply2(*mat2, max);
		std::vector<dense_matrix::ptr> ress(2);
		ress[0] = res1;
		ress[1] = res2;
		materialize(ress);
		gettimeofday(&end, NULL);
		printf("min+max takes %.3f seconds\n", time_diff(start, end));
	}

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res1 = mat1->mapply2(*mat2, add);
		res1->materialize_self();
		dense_matrix::ptr res2 = mat1->mapply2(*mat2, sub);
		res2->materialize_self();
		gettimeofday(&end, NULL);
		printf("separate add+sub takes %.3f seconds\n", time_diff(start, end));
	}

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res1 = mat1->mapply2(*mat2, add);
		dense_matrix::ptr res2 = mat1->mapply2(*mat2, sub);
		std::vector<dense_matrix::ptr> ress(2);
		ress[0] = res1;
		ress[1] = res2;
		materialize(ress);
		gettimeofday(&end, NULL);
		printf("add+sub takes %.3f seconds\n", time_diff(start, end));
	}
}

void test_conv_layout()
{
	struct timeval start, end;
	typedef size_t ele_type;
	size_t height = 1024 * 1024 * 256;
	size_t width = 32;

	dense_matrix::ptr mat = dense_matrix::create(height, width,
			matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res = mat->conv2(matrix_layout_t::L_COL);
		res->materialize_self();
		gettimeofday(&end, NULL);
		printf("conv layout (row to col) takes %.3f seconds\n",
				time_diff(start, end));
	}

	mat = dense_matrix::create(height, width, matrix_layout_t::L_COL,
			get_scalar_type<ele_type>(), mat_init<ele_type>(),
			matrix_conf.get_num_nodes());
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res = mat->conv2(matrix_layout_t::L_ROW);
		res->materialize_self();
		gettimeofday(&end, NULL);
		printf("conv layout (col to row) takes %.3f seconds\n",
				time_diff(start, end));
	}
}

void test_copy()
{
	struct timeval start, end;
	typedef size_t ele_type;
	size_t height = 1024 * 1024 * 1024;
	size_t width = 8;

	dense_matrix::ptr mat = dense_matrix::create(height, width,
			matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res = mat->deep_copy();
		gettimeofday(&end, NULL);
		printf("copy takes %.3f seconds\n", time_diff(start, end));
	}
}

void test_reset_data()
{
	struct timeval start, end;
	typedef size_t ele_type;
	size_t height = 1024 * 1024 * 1024;
	size_t width = 8;

	detail::matrix_store::ptr mat = detail::matrix_store::create(height, width,
				matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
				matrix_conf.get_num_nodes(), true);

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		mat->reset_data();
		gettimeofday(&end, NULL);
		printf("reset takes %.3f seconds\n", time_diff(start, end));
	}
}

void test_set_data()
{
	struct timeval start, end;
	typedef size_t ele_type;
	size_t height = 1024 * 1024 * 1024;
	size_t width = 8;

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr mat = dense_matrix::create(height, width,
				matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
				mat_init<ele_type>(), matrix_conf.get_num_nodes());
		gettimeofday(&end, NULL);
		printf("setdata takes %.3f seconds\n", time_diff(start, end));
	}
}

class single_operate2
{
public:
	typedef std::shared_ptr<const single_operate2> const_ptr;

	virtual void run(const void *in1, const void *in2, void *out) const = 0;
};

template<class T>
class multiply2_op: public single_operate2
{
public:
	void run(const void *in1, const void *in2, void *out) const {
		const T *t_in1 = (const T *) in1;
		const T *t_in2 = (const T *) in2;
		T *t_out = (T *) out;
		*t_out = *t_in1 * *t_in2;
	}
};

template<class T>
class add2_op: public single_operate2
{
public:
	void run(const void *in1, const void *in2, void *out) const {
		const T *t_in1 = (const T *) in1;
		const T *t_in2 = (const T *) in2;
		T *t_out = (T *) out;
		*t_out = *t_in1 + *t_in2;
	}
};

static const size_t LONG_DIM_LEN = 1024;

class portion_multiply_op: public detail::portion_mapply_op
{
	single_operate2::const_ptr left_op;
	single_operate2::const_ptr right_op;
	detail::mem_col_matrix_store::const_ptr right;
public:
	portion_multiply_op(single_operate2::const_ptr left_op,
			single_operate2::const_ptr right_op,
			detail::mem_col_matrix_store::const_ptr right,
			size_t out_num_rows): detail::portion_mapply_op(
				out_num_rows, right->get_num_cols(), right->get_type()) {
		this->left_op = left_op;
		this->right_op = right_op;
		this->right = right;
	}

	void run(const detail::local_row_matrix_store &in,
			detail::local_matrix_store &out) const {
		std::vector<char> tmp(in.get_entry_size());
		for (size_t i = 0; i < in.get_num_rows(); i++) {
			const char *left_row = in.get_row(i);
			for (size_t j = 0; j < right->get_num_cols(); j++) {
				const char *right_col = right->get_col(j);
				char *res = out.get(i, j);
				left_op->run(left_row, right_col, res);
				// Work on the row from the left matrix and the column from
				// the right matrix
				for (size_t k = 1; k < right->get_num_rows(); k++) {
					left_op->run(left_row + k * in.get_entry_size(),
							right_col + k * in.get_entry_size(), tmp.data());
					right_op->run(tmp.data(), res, res);
				}
			}
		}
	}

	virtual void run(
			const std::vector<detail::local_matrix_store::const_ptr> &ins,
			detail::local_matrix_store &out) const {
			// If the local matrix isn't virtual, we don't need to resize it
			// to increase CPU cache hits.
			run(static_cast<const detail::local_row_matrix_store &>(*ins[0]),
					out);
	}

	virtual detail::portion_mapply_op::const_ptr transpose() const {
		return detail::portion_mapply_op::const_ptr();
	}
	virtual std::string to_string(
			const std::vector<detail::matrix_store::const_ptr> &mats) const {
		return std::string();
	}
};

template<class T>
class portion_multiply_op2: public detail::portion_mapply_op
{
	detail::mem_col_matrix_store::const_ptr right;
public:
	portion_multiply_op2(detail::mem_col_matrix_store::const_ptr right,
			size_t out_num_rows): detail::portion_mapply_op(
				out_num_rows, right->get_num_cols(), right->get_type()) {
		this->right = right;
	}

	void run(const detail::local_row_matrix_store &in,
			detail::local_matrix_store &out) const {
		const T *left_arr = (const T *) in.get_raw_arr();
		assert(left_arr);
		const T *right_arr = (const T *) right->get_raw_arr();
		assert(right_arr);
		T *res_arr = (T *) out.get_raw_arr();
		assert(res_arr);
		for (size_t i = 0; i < in.get_num_rows(); i++) {
			const T *left_row = left_arr + i * in.get_num_cols();
			for (size_t j = 0; j < right->get_num_cols(); j++) {
				const T *right_col = right_arr + j * right->get_num_rows();
				T tmp = 0;
				for (size_t k = 0; k < right->get_num_rows(); k++)
					tmp += left_row[k] * right_col[k];
				res_arr[i * out.get_num_cols() + j] = tmp;
			}
		}
	}

	virtual void run(
			const std::vector<detail::local_matrix_store::const_ptr> &ins,
			detail::local_matrix_store &out) const {
			// If the local matrix isn't virtual, we don't need to resize it
			// to increase CPU cache hits.
			run(static_cast<const detail::local_row_matrix_store &>(*ins[0]),
					out);
	}

	virtual detail::portion_mapply_op::const_ptr transpose() const {
		return detail::portion_mapply_op::const_ptr();
	}
	virtual std::string to_string(
			const std::vector<detail::matrix_store::const_ptr> &mats) const {
		return std::string();
	}
};

template<class Type, int LEN>
class add_bulk_op: public bulk_operate
{
public:
	virtual void runAA(size_t num_eles, const void *left_arr1,
			const void *right_arr1, void *output_arr1) const {
		assert(LEN == num_eles);
		const Type *left_arr = (const Type *) left_arr1;
		const Type *right_arr = (const Type *) right_arr1;
		Type *output_arr = (Type *) output_arr1;
		for (size_t i = 0; i < num_eles; i++)
			output_arr[i] = left_arr[i] + right_arr[i];
	}

	virtual void runAE(size_t num_eles, const void *left_arr1,
			const void *right, void *output_arr1) const {
		assert(LEN == num_eles);
		const Type *left_arr = (const Type *) left_arr1;
		Type *output_arr = (Type *) output_arr1;
		Type entry = *(const Type *) right;
		for (size_t i = 0; i < num_eles; i++)
			output_arr[i] = left_arr[i] + entry;
	}

	virtual void runEA(size_t num_eles, const void *left,
			const void *right_arr1, void *output_arr1) const {
		assert(LEN == num_eles);
		Type entry = *(const Type *) left;
		const Type *right_arr = (const Type *) right_arr1;
		Type *output_arr = (Type *) output_arr1;
		for (size_t i = 0; i < num_eles; i++)
			output_arr[i] = entry + right_arr[i];
	}

	virtual void runAgg(size_t num_eles, const void *left_arr1,
			void *output) const {
		assert(LEN == num_eles);
		const Type *left_arr = (const Type *) left_arr1;
		Type res = left_arr[0];
		for (size_t i = 1; i < num_eles; i++)
			res = left_arr[i] + res;
		*(Type *) output = res;
	}

	virtual const scalar_type &get_left_type() const {
		return get_scalar_type<Type>();
	}
	virtual const scalar_type &get_right_type() const {
		return get_scalar_type<Type>();
	}
	virtual const scalar_type &get_output_type() const {
		return get_scalar_type<Type>();
	}
	virtual std::string get_name() const {
		return "+";
	}
};

template<class Type, int LEN>
class multiply_bulk_op: public bulk_operate
{
public:
	virtual void runAA(size_t num_eles, const void *left_arr1,
			const void *right_arr1, void *output_arr1) const {
		assert(LEN == num_eles);
		const Type *left_arr = (const Type *) left_arr1;
		const Type *right_arr = (const Type *) right_arr1;
		Type *output_arr = (Type *) output_arr1;
		for (size_t i = 0; i < LEN; i++)
			output_arr[i] = left_arr[i] * right_arr[i];
	}

	virtual void runAE(size_t num_eles, const void *left_arr1,
			const void *right, void *output_arr1) const {
		assert(LEN == num_eles);
		const Type *left_arr = (const Type *) left_arr1;
		Type *output_arr = (Type *) output_arr1;
		Type entry = *(const Type *) right;
		for (size_t i = 0; i < LEN; i++)
			output_arr[i] = left_arr[i] * entry;
	}

	virtual void runEA(size_t num_eles, const void *left,
			const void *right_arr1, void *output_arr1) const {
		assert(LEN == num_eles);
		Type entry = *(const Type *) left;
		const Type *right_arr = (const Type *) right_arr1;
		Type *output_arr = (Type *) output_arr1;
		for (size_t i = 0; i < LEN; i++)
			output_arr[i] = entry * right_arr[i];
	}

	virtual void runAgg(size_t num_eles, const void *left_arr1,
			void *output) const {
		assert(LEN == num_eles);
		const Type *left_arr = (const Type *) left_arr1;
		Type res = left_arr[0];
		for (size_t i = 1; i < LEN; i++)
			res = left_arr[i] * res;
		*(Type *) output = res;
	}

	virtual const scalar_type &get_left_type() const {
		return get_scalar_type<Type>();
	}
	virtual const scalar_type &get_right_type() const {
		return get_scalar_type<Type>();
	}
	virtual const scalar_type &get_output_type() const {
		return get_scalar_type<Type>();
	}
	virtual std::string get_name() const {
		return "*";
	}
};

namespace fm
{
extern bool inner_prod_conv;
};

void test_inner_prod(matrix_layout_t layout)
{
	struct timeval start, end;
	typedef size_t ele_type;
	size_t height = 1024 * 1024 * 1024;
	size_t width = 8;

	dense_matrix::ptr mat = dense_matrix::create(height, width,
			layout, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	dense_matrix::ptr small_mat = dense_matrix::create(width, width,
			matrix_layout_t::L_COL, get_scalar_type<ele_type>(), mat_init<ele_type>(), -1);
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res = mat->multiply(*small_mat);
		res->materialize_self();
		gettimeofday(&end, NULL);
		printf("it takes %.3f seconds\n", time_diff(start, end));
	}

	if (mat->store_layout() == matrix_layout_t::L_ROW) {
		printf("with hard-coded length\n");
		dense_matrix::ptr res1;
		for (size_t i = 0; i < 5; i++) {
			res1 = NULL;
			gettimeofday(&start, NULL);
			res1 = mat->inner_prod(*small_mat,
					bulk_operate::const_ptr(new multiply_bulk_op<size_t, 8>()),
					bulk_operate::const_ptr(new add_bulk_op<size_t, 8>()));
			res1->materialize_self();
			gettimeofday(&end, NULL);
			printf("it takes %.3f seconds\n", time_diff(start, end));
		}

		mat = mat->conv2(matrix_layout_t::L_COL);
		printf("conv layout\n");
		dense_matrix::ptr res2;
		for (size_t i = 0; i < 5; i++) {
			res2 = NULL;
			gettimeofday(&start, NULL);
			res2 = mat->multiply(*small_mat);
			res2->materialize_self();
			gettimeofday(&end, NULL);
			printf("it takes %.3f seconds\n", time_diff(start, end));
		}

		dense_matrix::ptr tmp = res1->minus(*res2);
		scalar_variable::ptr var = tmp->sum();
		assert(scalar_variable::get_val<ele_type>(*var) == 0);
	}
}

void test_inner_prod_manual()
{
	struct timeval start, end;
	typedef size_t ele_type;
	size_t height = 1024 * 1024 * 1024;
	size_t width = 8;

	dense_matrix::ptr mat = dense_matrix::create(height, width,
			matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	dense_matrix::ptr small_mat = dense_matrix::create(width, width,
			matrix_layout_t::L_COL, get_scalar_type<ele_type>(), mat_init<ele_type>(), -1);
	dense_matrix::ptr ret;
	printf("inner prod with single calls\n");
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		std::vector<detail::matrix_store::const_ptr> ins(1);
		ins[0] = mat->get_raw_store();
		detail::portion_mapply_op::const_ptr portion_op(new portion_multiply_op(
					single_operate2::const_ptr(new multiply2_op<ele_type>()),
					single_operate2::const_ptr(new add2_op<ele_type>()),
					std::dynamic_pointer_cast<const detail::mem_col_matrix_store>(
						small_mat->get_raw_store()),
					mat->get_num_rows()));
		detail::matrix_store::ptr res_store = __mapply_portion(ins, portion_op,
				mat->store_layout());
		gettimeofday(&end, NULL);
		printf("it takes %.3f seconds\n", time_diff(start, end));
		ret = dense_matrix::create(res_store);
	}

	printf("inner prod manually\n");
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		std::vector<detail::matrix_store::const_ptr> ins(1);
		ins[0] = mat->get_raw_store();
		detail::portion_mapply_op::const_ptr portion_op(
				new portion_multiply_op2<ele_type>(
					std::dynamic_pointer_cast<const detail::mem_col_matrix_store>(
						small_mat->get_raw_store()),
					mat->get_num_rows()));
		detail::matrix_store::ptr res_store = __mapply_portion(ins, portion_op,
				mat->store_layout());
		gettimeofday(&end, NULL);
		printf("it takes %.3f seconds\n", time_diff(start, end));
		ret = dense_matrix::create(res_store);
	}
}

void test_inner_prod()
{
	inner_prod_conv = false;
	test_inner_prod_manual();
	printf("inner prod row\n");
	test_inner_prod(matrix_layout_t::L_ROW);
	printf("inner prod col\n");
	test_inner_prod(matrix_layout_t::L_COL);
}

void test_multiply()
{
	struct timeval start, end;
	typedef double ele_type;
	size_t height = 1024 * 1024 * 1024;
	size_t width = 8;

	dense_matrix::ptr mat = dense_matrix::create(height, width,
			matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	dense_matrix::ptr small_mat = dense_matrix::create(width, width,
			matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	printf("matrix multiply (%ld x %ld) * (%ld x %ld)\n", mat->get_num_rows(),
			mat->get_num_cols(), small_mat->get_num_rows(),
			small_mat->get_num_cols());
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res = mat->multiply(*small_mat);
		res->materialize_self();
		gettimeofday(&end, NULL);
		printf("multiply tall takes %.3f seconds\n", time_diff(start, end));
	}

	dense_matrix::ptr mat1 = dense_matrix::create(width, height,
			matrix_layout_t::L_COL, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res = mat1->multiply(*mat);
		res->materialize_self();
		gettimeofday(&end, NULL);
		printf("multiply wide takes %.3f seconds\n", time_diff(start, end));
	}
}

void test_agg()
{
	struct timeval start, end;
	typedef size_t ele_type;
	size_t height = 1024L * 1024 * 1024 * 4;
	size_t width = 8;

	dense_matrix::ptr mat = dense_matrix::create(height, width,
			matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	bulk_operate::const_ptr min
		= bulk_operate::conv2ptr(*mat->get_type().get_basic_ops().get_op(basic_ops::op_idx::MIN));
	bulk_operate::const_ptr max
		= bulk_operate::conv2ptr(*mat->get_type().get_basic_ops().get_op(basic_ops::op_idx::MAX));
	agg_operate::const_ptr min_agg = agg_operate::create(min);
	agg_operate::const_ptr max_agg = agg_operate::create(max);

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		mat->aggregate(min);
		gettimeofday(&end, NULL);
		printf("min takes %.3f seconds\n", time_diff(start, end));
	}

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		std::vector<dense_matrix::ptr> res(2);
		res[0] = mat->aggregate(matrix_margin::BOTH, min_agg);
		res[1] = mat->aggregate(matrix_margin::BOTH, max_agg);
		materialize(res);
		gettimeofday(&end, NULL);
		printf("range takes %.3f seconds\n", time_diff(start, end));
	}

	bulk_operate::const_ptr add
		= bulk_operate::conv2ptr(*mat->get_type().get_basic_ops().get_op(basic_ops::op_idx::ADD));
	bulk_operate::const_ptr sub
		= bulk_operate::conv2ptr(*mat->get_type().get_basic_ops().get_op(basic_ops::op_idx::SUB));
	agg_operate::const_ptr add_agg = agg_operate::create(add);
	agg_operate::const_ptr sub_agg = agg_operate::create(sub);

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		mat->aggregate(add);
		gettimeofday(&end, NULL);
		printf("sum takes %.3f seconds\n", time_diff(start, end));
	}

	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		std::vector<dense_matrix::ptr> res(2);
		res[0] = mat->aggregate(matrix_margin::BOTH, add_agg);
		res[1] = mat->aggregate(matrix_margin::BOTH, sub_agg);
		materialize(res);
		gettimeofday(&end, NULL);
		printf("add+sub takes %.3f seconds\n", time_diff(start, end));
	}
}

void test_agg_dmultiply(matrix_layout_t layout)
{
	size_t num_tot = 16L * 1024 * 1024 * 1024;
	for (size_t i = 1; i < 8; i++) {
		size_t width = 1 << i;
		size_t height = num_tot / width;
		dense_matrix::ptr mat = dense_matrix::create(height, width, layout,
				get_scalar_type<double>(), mat_init<double>(),
				matrix_conf.get_num_nodes());
		agg_operate::const_ptr mul_agg = agg_operate::create(
				bulk_operate::conv2ptr(mat->get_type().get_basic_ops().get_multiply()));
		printf("start to compute\n");
		for (size_t i = 0; i < 5; i++) {
			struct timeval start, end;
			gettimeofday(&start, NULL);
			dense_matrix::ptr res = mat->aggregate(matrix_margin::MAR_ROW,
					mul_agg);
			res->materialize_self();
			assert(res->get_num_rows() == height);
			assert(res->get_num_cols() == 1);
			gettimeofday(&end, NULL);
			printf("agg on %ld x %ld matrix takes %f seconds\n",
					mat->get_num_rows(), mat->get_num_cols(),
					time_diff(start, end));
		}
	}
}

void test_agg_ladd(matrix_layout_t layout)
{
	size_t num_tot = 16L * 1024 * 1024 * 1024;
	for (size_t i = 1; i < 8; i++) {
		size_t width = 1 << i;
		size_t height = num_tot / width;
		dense_matrix::ptr mat = dense_matrix::create(height, width, layout,
				get_scalar_type<size_t>(), mat_init<size_t>(),
				matrix_conf.get_num_nodes());
		agg_operate::const_ptr add_agg = agg_operate::create(
				bulk_operate::conv2ptr(mat->get_type().get_basic_ops().get_add()));
		printf("start to compute\n");
		for (size_t i = 0; i < 5; i++) {
			struct timeval start, end;
			gettimeofday(&start, NULL);
			dense_matrix::ptr res = mat->aggregate(matrix_margin::MAR_ROW,
					add_agg);
			res->materialize_self();
			assert(res->get_num_rows() == height);
			assert(res->get_num_cols() == 1);
			gettimeofday(&end, NULL);
			printf("agg on %ld x %ld matrix takes %f seconds\n",
					mat->get_num_rows(), mat->get_num_cols(),
					time_diff(start, end));
		}
	}
}

void test_agg_mat()
{
	printf("test agg matrix\n");
	printf("test int add on column-major matrix\n");
	test_agg_ladd(matrix_layout_t::L_COL);
	printf("test int add on row-major matrix\n");
	test_agg_ladd(matrix_layout_t::L_ROW);

	printf("test float multiply on column-major matrix\n");
	test_agg_dmultiply(matrix_layout_t::L_COL);
	printf("test float multiply on row-major matrix\n");
	test_agg_dmultiply(matrix_layout_t::L_ROW);
}

void test_groupby()
{
}

void test_mapply_rows()
{
}

void test_mapply_cols()
{
}

void test_mapply2()
{
}

void test_sapply()
{
}

void test_apply_scalar()
{
}

class single_operate
{
public:
	typedef std::shared_ptr<const single_operate> const_ptr;

	virtual void run(const void *in, void *out) const = 0;
};

template<class T>
class add1_op: public single_operate
{
	T val;
public:
	add1_op(T val) {
		this->val = val;
	}

	void run(const void *in, void *out) const {
		const T *t_in = (const T *) in;
		T *t_out = (T *) out;
		*t_out = *t_in + val;
	}
};

class portion_add_op: public detail::portion_mapply_op
{
	single_operate::const_ptr op;
public:
	portion_add_op(single_operate::const_ptr op, size_t out_num_rows,
			size_t out_num_cols, const scalar_type &type): detail::portion_mapply_op(
				out_num_rows, out_num_cols, type) {
		this->op = op;
	}

	void run(const detail::local_matrix_store &in, detail::local_matrix_store &out) const {
		const char *in_raw = (const char *) in.get_raw_arr();
		assert(in_raw);
		char *out_raw = (char *) out.get_raw_arr();
		assert(out_raw);
		size_t tot_num = in.get_num_rows() * in.get_num_cols();
		size_t entry_size = get_output_type().get_size();
		for (size_t i = 0; i < tot_num; i++)
			op->run(in_raw + i * entry_size, out_raw + i * entry_size);
	}

	virtual void run(
			const std::vector<detail::local_matrix_store::const_ptr> &ins,
			detail::local_matrix_store &out) const {
		if (ins[0]->is_virtual()) {
			size_t orig_num_rows = ins[0]->get_num_rows();
			detail::local_matrix_store::exposed_area orig_in
				= ins[0]->get_exposed_area();
			detail::local_matrix_store::exposed_area orig_res
				= out.get_exposed_area();
			detail::local_matrix_store &mutable_store
				= const_cast<detail::local_matrix_store &>(*ins[0]);
			for (size_t row_idx = 0; row_idx < orig_num_rows;
					row_idx += LONG_DIM_LEN) {
				size_t llen = std::min(orig_num_rows - row_idx, LONG_DIM_LEN);
				mutable_store.resize(orig_in.local_start_row + row_idx,
						orig_in.local_start_col, llen, ins[0]->get_num_cols());
				out.resize(orig_res.local_start_row + row_idx,
						orig_res.local_start_col, llen, ins[0]->get_num_cols());
				run(*ins[0], out);
			}
			mutable_store.restore_size(orig_in);
			out.restore_size(orig_res);
		}
		else
			// If the local matrix isn't virtual, we don't need to resize it
			// to increase CPU cache hits.
			run(*ins[0], out);
	}

	virtual detail::portion_mapply_op::const_ptr transpose() const {
		return detail::portion_mapply_op::const_ptr();
	}
	virtual std::string to_string(
			const std::vector<detail::matrix_store::const_ptr> &mats) const {
		return std::string();
	}
};

void test_VUDF()
{
	// This is to demonstrate the overhead of function calls and
	// the effectiveness of VUDF.
	// We need to be careful. We might saturate memory bandwidth before
	// seeing the benefit of VUDF.
	typedef char ele_type;
	size_t height = 1024 * 1024 * 1024;
	size_t width = 16;
	dense_matrix::ptr mat = dense_matrix::create(height, width,
			matrix_layout_t::L_ROW, get_scalar_type<ele_type>(),
			mat_init<ele_type>(), matrix_conf.get_num_nodes());
	printf("sapply\n");
	for (size_t pipe_len = 1; pipe_len < 128; pipe_len *= 2) {
		dense_matrix::ptr res1;
		for (size_t i = 0; i < 5; i++) {
			res1 = NULL;
			struct timeval start, end;
			gettimeofday(&start, NULL);
			res1 = mat->add_scalar<ele_type>(1);
			for (size_t k = 0; k < pipe_len - 1; k++)
				res1 = res1->add_scalar<ele_type>(1);
			res1->materialize_self();
			gettimeofday(&end, NULL);
			printf("add %ld on %ld x %ld matrix takes %.3f seconds\n", pipe_len,
					mat->get_num_rows(), mat->get_num_cols(), time_diff(start, end));
		}
		if (mat->get_type() == get_scalar_type<size_t>()) {
			dense_matrix::ptr tmp = res1->minus(*mat);
			tmp = tmp->minus_scalar<ele_type>(pipe_len);
			scalar_variable::ptr var = tmp->sum();
			assert(scalar_variable::get_val<size_t>(*var) == 0);
		}
	}

	printf("single add\n");
	for (size_t pipe_len = 1; pipe_len < 128; pipe_len *= 2) {
		dense_matrix::ptr res1;
		for (size_t i = 0; i < 5; i++) {
			res1 = NULL;
			struct timeval start, end;
			gettimeofday(&start, NULL);
			std::vector<detail::matrix_store::const_ptr> ins(1);
			ins[0] = mat->get_raw_store();
			detail::portion_mapply_op::const_ptr portion_op(new portion_add_op(
						single_operate::const_ptr(new add1_op<ele_type>(1)),
						mat->get_num_rows(), mat->get_num_cols(), mat->get_type()));
			detail::matrix_store::ptr res_store = __mapply_portion_virtual(ins, portion_op,
					mat->store_layout());
			for (size_t k = 0; k < pipe_len - 1; k++) {
				ins[0] = res_store;
				res_store = __mapply_portion_virtual(ins, portion_op, mat->store_layout());
			}
			res1 = dense_matrix::create(res_store);
			res1->materialize_self();
			gettimeofday(&end, NULL);
			printf("add %ld on %ld x %ld matrix takes %.3f seconds\n", pipe_len,
					mat->get_num_rows(), mat->get_num_cols(), time_diff(start, end));
		}
		if (mat->get_type() == get_scalar_type<size_t>()) {
			dense_matrix::ptr tmp = res1->minus(*mat);
			tmp = tmp->minus_scalar<ele_type>(pipe_len);
			scalar_variable::ptr var = tmp->sum();
			assert(scalar_variable::get_val<size_t>(*var) == 0);
		}
	}
}

void test_crossprod()
{
	size_t long_dim = 100000000;
	size_t short_dim = 32;
	struct timeval start, end;
	set_operate::const_ptr init_op = create_nrand_init<double>(1, 1);
	dense_matrix::ptr mat1, mat2;

	printf("crossprod in-mem col\n");
	mat1 = dense_matrix::create(long_dim, short_dim, matrix_layout_t::L_COL,
			get_scalar_type<double>(), *init_op, matrix_conf.get_num_nodes(),
			true);

	mat2 = mat1->transpose();
	printf("t(mat) * mat\n");
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res = mat2->multiply(*mat1);
		res->materialize_self();
		gettimeofday(&end, NULL);
		printf("it takes %.3f seconds\n", time_diff(start, end));
	}

	printf("crossprod in-mem row\n");
	mat1 = dense_matrix::create(long_dim, short_dim, matrix_layout_t::L_ROW,
			get_scalar_type<double>(), *init_op, matrix_conf.get_num_nodes(),
			true);

	mat2 = mat1->transpose();
	printf("t(mat) * mat\n");
	for (size_t i = 0; i < 5; i++) {
		gettimeofday(&start, NULL);
		dense_matrix::ptr res = mat2->multiply(*mat1);
		res->materialize_self();
		gettimeofday(&end, NULL);
		printf("it takes %.3f seconds\n", time_diff(start, end));
	}
}

int main(int argc, char *argv[])
{
	if (argc < 3) {
		fprintf(stderr, "test conf_file test_name\n");
		return -1;
	}
	std::string conf_file = argv[1];
	std::string test_name = argv[2];
	config_map::ptr configs = config_map::create(conf_file);
	init_flash_matrix(configs);

	if (test_name == "conv_layout")
		test_conv_layout();
	else if (test_name == "copy")
		test_copy();
	else if (test_name == "set_data")
		test_set_data();
	else if (test_name == "reset_data")
		test_reset_data();
	else if (test_name == "inner_prod")
		test_inner_prod();
	else if (test_name == "multiply")
		test_multiply();
	else if (test_name == "agg")
		test_agg();
	else if (test_name == "agg_mat")
		test_agg_mat();
	else if (test_name == "groupby")
		test_groupby();
	else if (test_name == "mapply_rows")
		test_mapply_rows();
	else if (test_name == "mapply_cols")
		test_mapply_cols();
	else if (test_name == "mapply2")
		test_mapply2();
	else if (test_name == "sapply")
		test_sapply();
	else if (test_name == "apply_scalar")
		test_apply_scalar();
	else if (test_name == "mapply")
		test_mapply();
	else if (test_name == "VUDF")
		test_VUDF();
	else if (test_name == "crossprod")
		test_crossprod();
	else {
		fprintf(stderr, "unknow test\n");
		return -1;
	}
	destroy_flash_matrix();
}
