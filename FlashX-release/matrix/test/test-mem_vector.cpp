#include <boost/format.hpp>

#include "log.h"
#include "common.h"

#include "mem_vec_store.h"
#include "vector.h"

using namespace fm;

detail::smp_vec_store::ptr get(const detail::smp_vec_store &vec,
		detail::smp_vec_store &idxs)
{
	detail::smp_vec_store::ptr ret = detail::smp_vec_store::create(idxs.get_length(),
			vec.get_type());
#pragma omp parallel for
	for (size_t i = 0; i < idxs.get_length(); i++) {
		off_t idx = idxs.get<off_t>(i);
		// Check if it's out of the range.
		if (idx < 0 && (size_t) idx >= vec.get_length()) {
			BOOST_LOG_TRIVIAL(error)
				<< boost::format("%1% is out of range") % idx;
			continue;
		}

		ret->set(i, vec.get<int>(idx));
	}
	return ret;
}

/*
 * This is to measure the performance difference of random permutation
 * with and without compile-time type.
 */
void test_permute()
{
	printf("test permutation\n");
	detail::smp_vec_store::ptr vec = detail::smp_vec_store::create(1000000000,
			get_scalar_type<int>());
	for (size_t i = 0; i < vec->get_length(); i++)
		vec->set(i, random());
	detail::smp_vec_store::ptr clone = detail::smp_vec_store::cast(vec->deep_copy());
	vector::ptr vec1 = vector::create(clone);
	vector::ptr vec2 = vector::create(vec);
	assert(vec1->equals(*vec2));

	struct timeval start, end;
	gettimeofday(&start, NULL);
	detail::smp_vec_store::ptr idxs = detail::smp_vec_store::cast(
			vec->sort_with_index());
	gettimeofday(&end, NULL);
	printf("sort takes %fseconds\n", time_diff(start, end));

	gettimeofday(&start, NULL);
	// This has compile-time type.
	detail::smp_vec_store::ptr sorted1 = get(*clone, *idxs);
	gettimeofday(&end, NULL);
	printf("permute with type takes %fseconds\n", time_diff(start, end));

	gettimeofday(&start, NULL);
	// This doesn't have compile-time type.
	detail::smp_vec_store::ptr sorted2 = clone->detail::smp_vec_store::get(*idxs);
	gettimeofday(&end, NULL);
	printf("permute without type takes %fseconds\n", time_diff(start, end));
	vec1 = vector::create(sorted1);
	vec2 = vector::create(sorted2);
	assert(vec1->equals(*vec2));
}

int main()
{
	test_permute();
}
