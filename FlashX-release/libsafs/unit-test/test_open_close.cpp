#include "io_interface.h"
#include "thread.h"
#include "safs_file.h"

using namespace safs;

class test_thread: public thread
{
	std::vector<std::string> data_files;
public:
	test_thread(const std::vector<std::string> &files, int start,
			int end): thread("test", 0) {
		data_files.assign(files.begin() + start, files.begin() + end);
	}

	void run() {
		for (unsigned i = 0; i < data_files.size(); i++) {
			create_io_factory(data_files[i], REMOTE_ACCESS);
		}
		this->stop();
	}
};

int main(int argc, char *argv[])
{
	const int NUM_FILES = 1000;
	const int NUM_THREADS = 10;

	const char *opts[1];
	std::string opt = std::string("root_conf=") + argv[1];
	opts[0] = opt.c_str();
	config_map::ptr configs = config_map::create();
	configs->add_options(opts, 1);

	init_io_system(configs);
	const RAID_config &raid = get_sys_RAID_conf();
	std::vector<std::string> data_files;
	for (int i = 0; i < NUM_FILES; i++) {
		std::string file_name = std::string("test-") + itoa(i);
		safs_file file(raid, file_name);
		assert(file.create_file(0));
		data_files.push_back(file_name);
	}

	std::vector<thread *> threads;
	int num_files_per_thread = NUM_FILES / NUM_THREADS;
	for (int i = 0; i < NUM_THREADS; i++) {
		thread *t = new test_thread(data_files, i * num_files_per_thread,
				(i + 1) * num_files_per_thread);
		t->start();
		threads.push_back(t);
	}
	for (int i = 0; i < NUM_THREADS; i++)
		threads[i]->join();

	for (int i = 0; i < NUM_FILES; i++) {
		std::string file_name = data_files[i];
		safs_file file(raid, file_name);
		assert(file.delete_file());
	}
}
