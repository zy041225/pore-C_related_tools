# Tools to process pore-C data

# greedyly extract pairwise contact from BAM
`python extract_pairs.greedy.v1.1.py ref.fa.fai input.bam | gzip > output.pairs.gz`

## add simulated enzyme digestion site to pair, column 9 and column 10, therefore determining which contact is valid
`python check_valid_pairs.v1.py output.sort.pairs.gz DpnII.bed output.sort.add.pairs.gz`

# count the number of total contacts, valid contacts, intra and inter contacts
`python count_valid_pairs.py output.sort.add.pairs.gz > output.sort.add.pairs.gz.stat`

# calculate minimum resolution of the pairs file
`python calculate_resolution_for_pairs.py output.sort.add.valid.pairs.gz > output.sort.add.valid.pairs.gz.resolution`

# split inter and intra from the input pairs file
`python split_inter_intra.py output.sort.add.valid.pairs.gz ./`
