#!/usr/bin/env python3

import sys
import itertools
import random
import pickle
from collections import defaultdict
import pysam

if len(sys.argv) != 3:
    sys.exit('python3 %s <ref.fa.fai> <inFile.bam>' % (sys.argv[0]))

faiFile = sys.argv[1]
inFile = sys.argv[2]

print('## pairs format v1.0')
dic = dict()
with open(faiFile) as f:
    for line in f:
        line = line.rstrip()
        tmp = line.split('\t')
        rid = tmp[0]
        l = int(tmp[1])
        print(f'#chromsize: {rid} {l}')
print('#columns: readID chr1 pos1 chr2 pos2 strand1 strand2 pair_type')

lst = []

if inFile.endswith('.bam'):
    samfile = pysam.AlignmentFile(inFile, "rb")
else:
    samfile = pysam.AlignmentFile(inFile, "r")

readID = ''
last_rid = ''
last_start = 0
last_end = 0
last_strand = ''
last_mapq = 0

for read in samfile:
    if not read.is_unmapped:
        if read.query_name.split(':')[0] == readID:
            readID = read.query_name.split(':')[0]
            rid = read.reference_name
            start = read.reference_start+1
            end = read.reference_end
            if read.is_reverse:
                strand = '-'
            else:
                strand = '+'
            mapq = read.mapping_quality
            info = '%s#%s#%s' % (rid, start, strand)
            lst.append(info)
        else:
            if readID != '':
                for x in itertools.combinations(lst, 2):
                    tmp0 = x[0].split('#')
                    tmp1 = x[1].split('#')
                    #print(f'{readID}\t{rid0\tstart0\trid1\t{start1}\t')
                    print(f'{readID}\t{tmp0[0]}\t{tmp0[1]}\t{tmp1[0]}\t{tmp1[1]}\t{tmp0[2]}\t{tmp1[2]}\tUU')
            lst = []
            readID = read.query_name.split(':')[0]
            rid = read.reference_name
            start = read.reference_start
            end = read.reference_end
            if read.is_reverse:
                strand = '-'
            else:
                strand = '+'
            mapq = read.mapping_quality
            info = '%s#%s#%s' % (rid, start, strand)
            lst.append(info)
             
        readID = read.query_name.split(':')[0]

for x in itertools.combinations(lst, 2):
    tmp0 = x[0].split('#')
    tmp1 = x[1].split('#')
    print(f'{readID}\t{tmp0[0]}\t{tmp0[1]}\t{tmp1[0]}\t{tmp1[1]}\t{tmp0[2]}\t{tmp1[2]}\tUU')
