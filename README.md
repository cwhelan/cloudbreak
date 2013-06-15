## cloudbreak

Cloudbreak is a Hadoop-based structural variation (SV) caller for Illumina
paired-end DNA sequencing data. Currently Cloudbreak calls genomic insertions
and deletions; we are working on adding support for other types of SVs.

Cloudbreak contains a full pipeline for aligning your data in the form of FASTQ
files using alignment pipelines that generate many possible mappings for every
read, in the Hadoop framework. It then contains Hadoop jobs for computing
genomic features from the alignments, and for calling insertion and deletion
variants from those features.

# BUILDING FROM SOURCE

To build the latest version of Cloudbreak, clone the gitub repository. You'll
need to install Maven to build the executables. (http://maven.apache.org/)
Enter the top level directory of the Cloudbreak repository and tyoe command:

`mvn package`

This should compile the code, execute tests, and create the final jar file
in the `target/` directory.

#DEPENDENCIES

Cloudbreak requires a cluster Hadoop 0.20.2 or Cloudera CDH3 to run (the older
mapreduce API). If you don't have a Hadoop cluster, Cloudbreak can also use the
Apache Whirr API to automatically provision a cluster on the Amazon Elastic
Compute Cloud (EC2). See the section on using WHIRR below.

If you wish to run alignments using Cloudbreak, you will need one of the following
supported aligners:

* BWA (Recommended): http://bio-bwa.sourceforge.net/
* GEM: http://algorithms.cnag.cat/wiki/The_GEM_library
* RazerS 3: http://www.seqan.de/projects/razers/
* Bowtie2: http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
* Novoalign: http://www.novocraft.com

#USER GUIDE

You can use Cloudbreak in several different ways, depending on whether you want
to start with FASTQ files and use Hadoop to create alignments, or if you already
have an aligned BAM file and just want to use Cloudbreak to call variants. In addition,
the workflow is slightly different depending on whether you want to run on a local
hadoop cluster or want to run using a cloud provider like Amazon EC2. Find the scenario
below that best fits your use case for more details on how to run that workflow. For
each scenario, we have created a template script that contains all of the steps
and parameters you need, which you can modify for your particular data set.

#SCENARIO 1: Compute alignments in Hadoop, using a local Hadoop cluster

To install aligner dependencies for use by Cloudbreak, first generate the index
for the genome reference you would like to run against. Then, copy all of the
required index files, and the executable files for the aligner into HDFS using
the `hadoop dfs -copyFromLocal` command. For BWA you will need all of the index files
created by running `bwa index`. You will also need an 'fai' file for the reference,
containing chromosome names and lengths, generated by `samtools faidx`.

If your reference file is `reference.fa`, and `bwa aln` has created the files

    `reference.fa.amb`
    `reference.fa.ann`
    `reference.fa.bwt`
    `reference.fa.pac`
    `reference.fa.sa`

and `reference.fa.fai` as described above, issue the following
commands to load the necessary files into HDFS:

    hdfs -mkdir indices/
    hdfs -mkdir executables/
    hdfs -copyFromLocal reference.fa.amb indices/
    hdfs -copyFromLocal reference.fa.ann indices/
    hdfs -copyFromLocal reference.fa.bwt indices/
    hdfs -copyFromLocal reference.fa.pac indices/
    hdfs -copyFromLocal reference.fa.sa indices/
    hdfs -copyFromLocal reference.fa.fai indices/
    hdfs -copyFromLocal /path/to/bwa/executables/bwa executables/
    hdfs -copyFromLocal /path/to/bwa/executables/xa2multi.pl executables/

The basic workflow is:

1. Load the FASTQ files into HDFS
2. Run one of the Cloudbreak alignment commands to align your reads
3. Create a readGroup file to describe the location and insert size characteristics of your reads, and copy it into HDFS.
4. Run the GMM fitting feature generation step of the Cloudbreak process.
5. Extract deletion calls from the features created in step 4.
6. Copy the deletion calls from HDFS to a local directory.
5. Extract insertion calls from the features created in step 4.
6. Copy the insertion calls from HDFS to a local directory.
7. Optionally, export the alignments back into a BAM file in your local filesystem.

We have created a script to run through the full process of executing the
Cloudbreak pipeline from FASTQ files to insertion and deletion calls. The
script is named `Cloudbreak-full.sh` and can be found in the scripts directory
of the Cloudbreak distribution. To customize the script for your needs, copy
it to a new location and edit the variables in the first three sections:
"EXPERIMENT DETAILS", "LOCAL FILES AND DIRECTORIES", and
"HDFS FILES AND DIRECTORIES".

#SCENARIO 2: Call variants on existing alignments, using a local Hadoop cluster

For this scenario you don't need to worry about having an aligner executable or
aligner-generated reference in HDFS. You will however, need a chromosome length
'fai' file, which you can generate by running `samtools faidx` on your reference
FASTA files and then copying to HDFS:

    hdfs -copyFromLocal reference.fa.fai indices/

After that, the workflow is:

1. Load your BAM file into HDFS and prepare it for Cloudbreak
2. Create a readGroup file to describe the location and insert size characteristics of your reads.
3. Run the GMM fitting feature generation step of the Cloudbreak process.
4. Extract deletion calls from the features created in step 3.
5. Copy the deletion calls from HDFS to a local directory.
6. Extract insertion calls from the features created in step 3.
7. Copy the insertion calls from HDFS to a local directory.

The fastest way to accomplish step 1 may vary depending on the type of BAM file you have.
To prepare alignments for Cloudbreak, they must be sorted by read name. If you already have a
 BAM file in which the reads have been sorted by name (using `samtools sort -n` or a similar
 command), your fastest bet is to load it into HDFS directly using the `readSAMFileIntoHDFS`
 Cloudbreak command. If you don't already have a name-sorted BAM file, it can be faster
 to sort your alignments using Hadoop (Insert example here).

Templates for both of these scenarios are available in the script `Cloudbreak-variants-only.sh`
located in the scripts directory of the Cloudbreak distribution.

#SCENARIO 3: Compute alignments in Hadoop, using a cloud provider like EC2

#SCENARIO 4: Call variants on existing alignments, using a cloud provider like EC2

#OUTPUT FILES

The output from Cloudbreak will be found in the files named

*READ_GROUP_LIBRARY_dels_genotyped.bed
*READ_GROUP_LIBRARY_ins_genotyped.bed

where READ_GROUP and LIBRARY are the names of the reads in your experiment. The
format of the files is tab-delimited with the following columns:

*CHROMOSOME: The chromosome of the deletion call
*START: The start coordinate of the deletion call
*END: The end coordinate of the deletion call
*NUMBER: The cloudbreak identifier of the deletion call
*LR: The likelihood ratio of the deletion (higher indicates a call more likely to be true)
*TYPE: Either "INS" or "DEL"
*W: The average weight of the estimated GMM mixing parameter alpha, used in genotyping
*GENOTYPE: The predicted genotype of the call

#RUNNING ON EC2 WITH WHIRR

Cloudbreak has limited support for automatically deploying a Hadoop cluster on
Amazon EC2, transferring your data there, running the Cloudbreak algorithm, and
downloading the results. Of course, renting compute time on EC2 costs money, so
we recommend that you be familiar with EC2 usage and billing before attempting this.
WE ARE NOT RESPONSIBLE FOR UNEXPECTED CHARGES THAT YOU MIGHT INCUR ON EC2.

You can see an example workflow involving EC2 by examining the script
`Cloudbreak-EC2-whirr.sh`. This begins by transferring your reads to Amazon S3. It then
uses Apache Whirr to launch an EC2 Hadoop cluster, copies the necessary executable files
to EC2, and run the algorithm. Finally, Cloudbreak will download the results from
the cloud and destroy the cluster.

Many properties that affect the cluster created can be set in the file
cloudbreak-whirr.properties. You will need to edit this file to set your AWS
access key and secret access key, and to tell it the location of the public and
private SSH keys to use to access the cluster. You can also control the number
and type of nodes to include in the cluster. The default settings in the file
create 40 nodes of type m2.xlarge, which is sufficient to fully process a 30X
simulation of human chromsome 2, including read alignment, in about 90 minutes.
You can also direct Whirr to use Amazon EC2's spot instances, which are
dramatically cheaper than on-demand instances, although they carry the risk of
being terminated if your price is out-bid. Using recent spot pricing, it cost
us about $5 to run the aforementioned chromosome 2 simulation. We recommend
setting your spot bid price to be the on demand price for the instance type you
are using to minimize the chance of having your instances terminated.

Please consult Amazon's EC2 documentation and the documentation for Whirr for
more information on how to configure and deploy clusters in the cloud.

#CONTACT INFORMATION

Please contact cwhelan@gmail.com with any questions on running cloudbreak.

#REFERENCE GUIDE

All of Cloudbreak's functionality is contained in the executable jar file in the
directory where you unpacked the Cloudbreak distribution. Use the 'hadoop'
command to run the jar file to ensure that the necessary Hadoop dependencies
are available to Cloudbreak.

Each command is detailed below; you can view usage information by typing
'hadoop jar cloudbreak-${project.version}.jar' without any additional parameters:

