/*
 * Copyright 2014 Open Connectome Project (http://openconnecto.me)
 * Written by Da Zheng (zhengda1936@gmail.com)
 *
 * This file is part of SAFSlib.
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

#include <system_error>

#include "read_private.h"
#include "file_mapper.h"

namespace safs
{

buffered_io::buffered_io(const logical_file_partition &partition_, thread *t,
		const safs_header &header, int flags): io_interface(t, header), partition(
			partition_), fds(partition.get_num_files())
{
	this->flags = flags;

	for (int i = 0; i < partition.get_num_files(); i++) {
		int ret;
		fds[i] = open(partition.get_file_name(i).c_str(), flags);
		if (fds[i] < 0) {
			char err_msg[128];
			snprintf(err_msg, sizeof(err_msg),
					"open %s", partition.get_file_name(i).c_str());
			perror(err_msg);
			throw std::system_error(std::make_error_code((std::errc) errno),
					"open");
		}
		ret = posix_fadvise(fds[i], 0, 0, POSIX_FADV_RANDOM);
		if (ret < 0)
			perror("posix_fadvise");
	}
}

io_status buffered_io::access(char *buf, off_t offset, ssize_t size, int access_method) {
	ASSERT_EQ(get_thread(), thread::get_curr_thread());
	int fd;
	if (fds.size() == 1)
		fd = fds[0];
	else {
		struct block_identifier bid;
		partition.map(offset / PAGE_SIZE, bid);
		fd = fds[bid.idx];
		offset = bid.off * PAGE_SIZE;
	}
	ssize_t ret;
	// TODO I need to make sure all data is read or written to the file.
	if (access_method == WRITE)
		ret = pwrite(fd, buf, size, offset);
	else
		ret = pread(fd, buf, size, offset);
	io_status status;
	if (ret < 0)
		status = IO_FAIL;
	else
		status = IO_OK;
	status.set_priv_data(ret);
	return status;
}

}
