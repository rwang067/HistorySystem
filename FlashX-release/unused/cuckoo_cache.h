#ifndef __CUCKOO_HASH_H__
#define __CUCKOO_HASH_H__

#include <stdlib.h>
#include <math.h>

#include "cache.h"

#ifdef STATISTICS
volatile extern int removed_indices;
#endif

#define MAXLOOP 2

class lockable_pointer
{
	static page_buffer<thread_safe_page> *buf;
	volatile int buf_idx;
public:
	lockable_pointer() {
		buf_idx = 0;
	}

	~lockable_pointer() {
	}

	static void set_buf(page_buffer<thread_safe_page> *b) {
		buf = b;
	}

	thread_safe_page *get_pointer() {
		return buf->get_page(buf_idx >> 1);
	}

	void lock() {
#ifdef DEBUG
		printf("thread %ld lock\n", pthread_self());
#endif
		while (__sync_fetch_and_or(&buf_idx, 0x1) & 0x1) {}
	}

	void unlock() {
#ifdef DEBUG
		printf("thread %ld unlock\n", pthread_self());
#endif
		__sync_fetch_and_and(&buf_idx, ~0x1);
	}

	void set_pointer(thread_safe_page *p) {
		if (p == NULL) {
			buf_idx = 0;
		}
		else {
			int idx = buf->get_idx(p);
			buf_idx = idx << 1 | (buf_idx & 0x1);
		}
	}
};

class cuckoo_hash
{
	lockable_pointer *tables[2];
	int log_table_sizes[2];
	long a[2];

	int hash(off_t key, int idx) {
		int v = ((a[idx] * key) & 0xFFFFFFFF) >> (32 - log_table_sizes[idx]);
		assert (v < (1 << log_table_sizes[idx]));
		return v;
	}
public:
	cuckoo_hash(int size) {
		const int table_size0 = size * 4;
		const int table_size1 = size * 2;
		/* cuckoo hash needs tables to be half-empty in order to be efficient. */
		tables[0] = new lockable_pointer[table_size0];
		tables[1] = new lockable_pointer[table_size1];
		log_table_sizes[0] = log2(table_size0);
		log_table_sizes[1] = log2(table_size1);
		a[0] = random();
		a[1] = random();
#ifdef STATISTICS
		removed_indices = 0;
#endif
	}

	~cuckoo_hash() {
		delete [] tables[0];
		delete [] tables[1];
	}

	thread_safe_page *swap_entry(const int i, const off_t key,
			thread_safe_page *const value);

	void insert(off_t key, thread_safe_page *value);

	bool remove_entry(const int i, const off_t key);

	void remove(off_t key) {
		if(!remove_entry(0, key))
			remove_entry(1, key);
	}

	thread_safe_page *search_entry(const int i, const off_t key);

	thread_safe_page *search(off_t key) {
		thread_safe_page *v = search_entry(0, key);
		if (v)
			return v;
		return search_entry(1, key);
	}
};

class cuckoo_cache: public page_cache
{
//	page_buffer *bufs;
	page_buffer<thread_safe_page> *buf;
	cuckoo_hash table;
public:
	cuckoo_cache(long cache_size): table(cache_size / PAGE_SIZE) {
		printf("cuckoo cache is used\n");
		long npages = cache_size / PAGE_SIZE;

//		/* each thread has a page buffer, and page eviction is done in the local thread. */
//		bufs = new page_buffer[nthreads](npages / nthreads);
		buf = new page_buffer<thread_safe_page>(npages, 0);
		lockable_pointer::set_buf(buf);
	}

	~cuckoo_cache() {
		delete buf;
//		delete [] bufs;
	}

	page *search(off_t offset, off_t &old_off);
};

#endif
