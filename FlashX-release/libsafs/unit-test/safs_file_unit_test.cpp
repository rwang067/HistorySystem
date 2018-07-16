#include "safs_file.h"
#include "RAID_config.h"

using namespace safs;

int main(int argc, char *argv[])
{
	RAID_config::ptr raid = RAID_config::create(argv[1], 0, 16);
	safs_file f(*raid, "test1");

	printf("Check the existence of %s\n", f.get_name().c_str());
	assert(!f.exist());

	printf("Create file %s\n", f.get_name().c_str());
	size_t file_size = 32 * 1024 * 1024;
	f.create_file(file_size);
	printf("%s has %ld bytes\n", f.get_name().c_str(), f.get_size());
	f.delete_file();
}
