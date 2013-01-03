#!/usr/bin/env python

import sys

sample_name = sys.argv[1]
seq_index_file = sys.argv[2]
hdfs_dir = sys.argv[3]
data_dir = sys.argv[4]
threshold = sys.argv[5]

seq_index = open(seq_index_file, 'r')

dag_file = open("align.dag", 'w')
read_group_file = open("readGroups.txt", 'w')

loaded_groups = set()

job_num = 1
for line in seq_index:
    fields = line.split("\t")
    file_name = fields[0]
    read_group = fields[2]
    library = fields[14]
    insert_size = fields[17]
    pair_file_name = fields[19]
    if (not (read_group in loaded_groups) and pair_file_name != ""):
        print "processing read group {0}".format(read_group)
        dag_file.write("JOB {0} loadAndAlign.desc\n".format(job_num))
        dag_file.write("VARS {0} read_group=\"{1}\"\n".format(job_num, read_group))
        dag_file.write("VARS {0} data_dir=\"{1}\"\n".format(job_num, data_dir))
        dag_file.write("VARS {0} hdfs_dir=\"{1}\"\n".format(job_num, hdfs_dir))
        dag_file.write("VARS {0} file1=\"{1}\"\n".format(job_num, file_name))
        dag_file.write("VARS {0} file2=\"{1}\"\n".format(job_num, pair_file_name))
        dag_file.write("VARS {0} threshold=\"{1}\"\n".format(job_num, threshold))
        loaded_groups.add(read_group)
        job_num = job_num + 1

        read_group_file.write("\t".join([read_group, library, insert_size, str(int(round(int(insert_size) * .15))), "false", hdfs_dir + "/alignments/" + read_group]) + "\n")
read_group_file.close()

job_desc_file = open("loadAndAlign.desc", 'w')
job_desc_file.write("Executable = loadAndAlign.sh\n")
job_desc_file.write("  Arguments  = \"$(read_group) $(data_dir) $(hdfs_dir) $(file1) $(file2) $(threshold)\"\n")
job_desc_file.write("  Universe   = vanilla\n")
job_desc_file.write("  output     = loadAndAlign.out.$(cluster).$(Process)\n")
job_desc_file.write("  error      = loadAndAlign.err.$(cluster).$(Process)\n")
job_desc_file.write("  Log        = /tmp/loadAndAlign.log\n")
job_desc_file.write("  get_env    = true\n")
job_desc_file.write("  Notification = Never\n")
job_desc_file.write("  Queue\n")
job_desc_file.close()

exe_file = open("loadAndAlign.sh", 'w')
exe_file.write("#!/bin/bash\n")
exe_file.write("\n")
exe_file.write("set -e\n")
exe_file.write("set -u\n")
exe_file.write("\n")
exe_file.write("READ_GROUP=$1\n")
exe_file.write("DATA_DIR=$2\n")
exe_file.write("HDFS_DIR=$3\n")
exe_file.write("FILE1=$4\n")
exe_file.write("FILE2=$5\n")
exe_file.write("THRESHOLD=$6\n")
exe_file.write("\n")
exe_file.write("hadoop jar /l2/users/whelanch/gene_rearrange/svpipeline/build/svpipeline/target/cloudbreak-1.0-SNAPSHOT-exe.jar readPairedEndFilesIntoHDFS --HDFSDataDir $HDFS_DIR/data/$READ_GROUP --fastqFile1 $DATA_DIR/$FILE1 --fastqFile2 $DATA_DIR/$FILE2 --compress snappy --outFileName $READ_GROUP\n")
exe_file.write("hadoop jar /l2/users/whelanch/gene_rearrange/svpipeline/build/svpipeline/target/cloudbreak-1.0-SNAPSHOT-exe.jar alignSingleEnds --HDFSDataDir $HDFS_DIR/data/$READ_GROUP --HDFSAlignmentsDir $HDFS_DIR/alignments/$READ_GROUP --reference indices/hg19.fa.nix --threshold $THRESHOLD --qualityFormat STDFQ --HDFSPathToNovoalign executables/novoalign --HDFSPathToNovoalignLicense executables/novoalign.lic\n")
exe_file.write("hadoop dfs -rmr $HDFS_DIR/data/$READ_GROUP\n")
exe_file.close()
