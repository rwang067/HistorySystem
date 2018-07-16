#########################################################################
# File Name: runPagerank.sh
# Author: zzqq2199
# Mail: zhouquanjs@qq.com
# Created Time: Sat 13 Jan 2018 09:06:40 AM DST
#########################################################################
#!/bin/bash

row_par=1
col_par=1
NUM_THDS=$((row_par*col_par*2))
beg_dir=../../../converter/LJ
csr_dir=../../../converter/LJ
beg_header=soc-LiveJournal1.txt-split_beg
csr_header=soc-LiveJournal1.txt-split_csr
num_chunks=16
chunk_sz=10240
io_limit=16
MAX_USELESS=16
ring_vert_count=16
num_buffs=16
iteration=10
walks_per_source=20

timeFileName=timeGraphene${beg_header}Iteration${iteration}WalksPerSource${walks_per_source}.log

echo 'st: '$(date +%T) >> ${timeFileName}

echo ./aio_pagerank.bin $row_par $col_par $NUM_THDS $beg_dir $csr_dir $beg_header $csr_header $num_chunks $chunk_sz $io_limit $MAX_USELESS $ring_vert_count $num_buffs $iteration $walks_per_source
sudo ./aio_pagerank.bin $row_par $col_par $NUM_THDS $beg_dir $csr_dir $beg_header $csr_header $num_chunks $chunk_sz $io_limit $MAX_USELESS $ring_vert_count $num_buffs $iteration $walks_per_source

echo 'et: '$(date +%T) >> ${timeFileName}

