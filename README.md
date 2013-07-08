#cloudbreak

Cloudbreak is a Hadoop-based structural variation (SV) caller for Illumina
paired-end DNA sequencing data. Currently Cloudbreak calls genomic insertions
and deletions; we are working on adding support for other types of SVs.

Cloudbreak contains a full pipeline for aligning your data in the form of FASTQ
files using alignment pipelines that generate many possible mappings for every
read, in the Hadoop framework. It then contains Hadoop jobs for computing
genomic features from the alignments, and for calling insertion and deletion
variants from those features.

You can get Cloudbreak by downloading a pre-packaged release from the "releases"
tab in the GitHub repository, or by building from source as described below.

##Building From Source

To build the latest version of Cloudbreak, clone the gitub repository. You'll
need to install Maven to build the executables. (http://maven.apache.org/)
Enter the top level directory of the Cloudbreak repository and type the command:

`mvn package`

This should compile the code, execute tests, and create a distribution file,
 `cloudbreak-$VERSION-dist.tar.gz` in the `target/` directory. You can then copy
 that distribution file to somewhere else on your system, unpack it with
 `tar -xzvf cloudbreak-$VERSION-dist.tar.gz` and access the Cloudbreak jar file
  and related scripts and properties files.

##Dependencies

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

##User Guide

You can use Cloudbreak in several different ways, depending on whether you want
to start with FASTQ files and use Hadoop to help parallelize your alignments, or if you already
have an aligned BAM file and just want to use Cloudbreak to call variants. In addition,
the workflow is slightly different depending on whether you want to run on a local
hadoop cluster or want to run using a cloud provider like Amazon EC2. Later in this
file, we've listed a set of scenarios to describe options for running the Cloudbreak
pipeline. Find the scenario that best fits your use case for more details on how to
run that workflow. For each scenario, we have created a template script that contains
all of the steps and parameters you need, which you can modify for your particular data set.

##Running on a cloud provider like Amazon EC2 with Whirr

Cloudbreak has support for automatically deploying a Hadoop cluster on
cloud providers such as Amazon EC2, transferring your data there, running the Cloudbreak algorithm, and
downloading the results.

Of course, renting compute time on EC2 or other clouds costs money, so please be
familiar with the appropriate usage and billing policies of your cloud provider
before attempting this.

WE ARE NOT RESPONSIBLE FOR UNEXPECTED CHARGES THAT YOU MIGHT INCUR ON EC2 OR
OTHER CLOUD PROVIDERS.

Many properties that affect the cluster created can be set in the file
`cloudbreak-whirr.properties` in this distribution. You will need to edit this file
to set your AWS access key and secret access key (or your credentials for other
cloud provider services), and to tell it the location of the public and
private SSH keys to use to access the cluster. You can also control the number
and type of nodes to include in the cluster. The default settings in the file
create 15 nodes of type m1.xlarge, which is sufficient to fully process a 30X
simulation of human chromsome 2, including read alignment and data transfer time,
in under an hour. We have only tested this capability using EC2; other cloud providers
may not work as well. You can also direct Whirr to use Amazon EC2's spot instances, which are
dramatically cheaper than on-demand instances, although they carry the risk of
being terminated if your price is out-bid. Using recent spot pricing, it cost
us about $5 to run the aforementioned chromosome 2 simulation. We recommend
setting your spot bid price to be the on demand price for the instance type you
are using to minimize the chance of having your instances terminated.

Please consult Amazon's EC2 documentation and the documentation for Whirr for
more information on how to configure and deploy clusters in the cloud.

##Running on a Small Example Data Set

To facilitate testing of Cloudbreak, we have publicly hosted the reads from the simulated
data example described in the Cloudbreak manuscript on a bucket in Amazon's S3 storage
 service at s3://cloudbreak-example/. We have also provided an example script that creates
 a cluster in Amazon EC2, copies the data to the cluster, runs the full Cloudbreak
  workflow including alignments with BWA, and copies the variant calls back to the
  local machine before destroying the cluster. The script, called `Cloudbreak-EC2-whirr-example.sh`
  is in the scripts directory of the Cloudbreak distribution. Of course, you will still
  need to edit the `cloudbreak-whirr.properties` file with your EC2 credentials, and verify
  that the cluster size, instance types, and spot price are to your liking before
  executing the example.

###Scenario 1: Compute alignments in Hadoop, using a local Hadoop cluster

To install aligner dependencies for use by Cloudbreak, first generate the index
for the genome reference you would like to run against. Then, copy all of the
required index files, and the executable files for the aligner into HDFS using
the `hadoop dfs -copyFromLocal` command. For BWA you will need all of the index files
created by running `bwa index`. You will also need an 'fai' file for the reference,
containing chromosome names and lengths, generated by `samtools faidx`.

If your reference file is `reference.fa`, and `bwa aln` has created the files

    reference.fa.amb
    reference.fa.ann
    reference.fa.bwt
    reference.fa.pac
    reference.fa.sa

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

###Scenario 2: Call variants on existing alignments, using a local Hadoop cluster

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

To prepare alignments for Cloudbreak, they must be sorted by read name. You can then use the
 `readSAMFileIntoHDFS` Cloudbreak command.

A templates for this scenario is available in the script `Cloudbreak-variants-only.sh`
located in the scripts directory of the Cloudbreak distribution.

###Scenario 3: Compute alignments in Hadoop, using a cloud provider like EC2

First, see the section "Running on a Cloud Provider like Amazon EC2 with Whirr" above, and modify the file
`cloudbreak-whirr.properties` to include your access credentials and the appropriate cluster
specifications. After that, the workflow is similar to the workflow described for scenario #1
above, whith the additionals first steps of copying your reads and dependency files to the cloud and
creating a cluster before processing begins, and then destroying the cluster after processing has
completed.

You can see an example workflow involving EC2 by examining the script
`Cloudbreak-EC2-whirr.sh`. This begins by transferring your reads to Amazon S3. It then
uses Apache Whirr to launch an EC2 Hadoop cluster, copies the necessary executable files
to EC2, and runs the algorithm.

###Scenario 4: Call variants on existing alignments, using a cloud provider like EC2

Again, please read the section "Running on a Cloud Provider like Amazon EC2 with Whirr" above to learn how to
update the `cloudbreak-whirr.properties` file with your credentials and cluster specifications. After that,
follow the template in the script `Cloudbreak-EC2-whirr-variants-only.sh` to create a workflow
involving calling variants in the cloud.

##Output Files

The output from running Cloudbreak using one of the scripts above will be found in the files named

    READ_GROUP_LIBRARY_dels_genotyped.bed
    READ_GROUP_LIBRARY_ins_genotyped.bed

where READ_GROUP and LIBRARY are the names of the reads in your experiment. The
format of the files is tab-delimited with the following columns:

*  CHROMOSOME: The chromosome of the deletion call
*  START: The start coordinate of the deletion call
*  END: The end coordinate of the deletion call
*  NUMBER: The cloudbreak identifier of the deletion call
*  LR: The likelihood ratio of the deletion (higher indicates a call more likely to be true)
*  TYPE: Either "INS" or "DEL"
*  W: The average weight of the estimated GMM mixing parameter alpha, used in genotyping
*  GENOTYPE: The predicted genotype of the call

##Contact information

Please contact cwhelan at gmail.com with any questions on running cloudbreak.

##Reference Guide

All of Cloudbreak's functionality is contained in the executable jar file in the
directory where you unpacked the Cloudbreak distribution. Use the 'hadoop'
command to run the jar file to ensure that the necessary Hadoop dependencies
are available to Cloudbreak.

To invoke any Cloudbreak command, use a command line in this format:

`hadoop cloudbreak-${project.version}.jar [options] [command] [command options]`

Where `command` is the name of the command, `command options` are the arguments specific
to that command, and `options` are general options, including options for how to run
Hadoop jobs. For example, if you'd like to specify 50 reduce tasks
for one of your commands, pass in `-Dmapred.reduce.tasks=50` as one of the general options. 

Each command is detailed below and its options are listed below. You can view this information by typing
`hadoop jar cloudbreak-${project.version}.jar` without any additional parameters.


        readPairedEndFilesIntoHDFS      Load paired FASTQ files into HDFS
          Usage: readPairedEndFilesIntoHDFS [options]
      Options:
            *     --HDFSDataDir                  HDFS directory to load reads into
                  --clipReadIdsAtWhitespace      Whether to clip all readnames at
                                                 the first whitespace (prevents trouble
                                                 with some aligners)
                                                 Default: true
                  --compress                     Compression codec to use on the
                                                 reads stored in HDFS
                                                 Default: snappy
            *     --fastqFile1                   File containing the first read in
                                                 each pair
            *     --fastqFile2                   File containing the second read in
                                                 each pair
                  --filesInHDFS                  Use this flag if the BAM file has
                                                 already been copied into HDFS
                                                 Default: false
                  --filterBasedOnCasava18Flags   Use the CASAVA 1.8 QC filter to
                                                 filter out read pairs
                                                 Default: false
                  --outFileName                  Filename of the prepped reads in
                                                 HDFS
                                                 Default: reads
                  --trigramEntropyFilter         Filter out read pairs where at
                                                 least one read has a trigram entropy less
                                                 than this value. -1 = no filter
                                                 Default: -1.0

        readSAMFileIntoHDFS      Load a SAM/BAM file into HDFS
          Usage: readSAMFileIntoHDFS [options]
      Options:
            *     --HDFSDataDir   HDFS Directory to hold the alignment data
                  --compress      Compression codec to use for the data
                                  Default: snappy
                  --outFileName   Filename to give the file in HDFS
                                  Default: alignments
            *     --samFile       Path to the SAM/BAM file on the local filesystem

        bwaPairedEnds      Run a BWA paired-end alignment
          Usage: bwaPairedEnds [options]
      Options:
            *     --HDFSAlignmentsDir    HDFS directory to hold the alignment data
            *     --HDFSDataDir          HDFS directory that holds the read data
            *     --HDFSPathToBWA        HDFS path to the bwa executable
                  --HDFSPathToXA2multi   HDFS path to the bwa xa2multi.pl executable
            *     --maxProcessesOnNode   Ensure that only a max of this many BWA
                                         processes are running on each node at once.
                                         Default: 6
                  --numExtraReports      If > 0, set -n and -N params to bwa sampe,
                                         and use xa2multi.pl to report multiple hits
                                         Default: 0
            *     --referenceBasename    HDFS path of the FASTA file from which the
                                         BWA index files were generated.

        novoalignSingleEnds      Run a Novoalign alignment in single ended mode
          Usage: novoalignSingleEnds [options]
      Options:
            *     --HDFSAlignmentsDir            HDFS directory to hold the
                                                 alignment data
            *     --HDFSDataDir                  HDFS directory that holds the read
                                                 data
            *     --HDFSPathToNovoalign          HDFS path to the Novoalign
                                                 executable
                  --HDFSPathToNovoalignLicense   HDFS path to the Novoalign license
                                                 filez
                  --qualityFormat                Quality score format of the FASTQ
                                                 files
                                                 Default: ILMFQ
            *     --reference                    HDFS path to the Novoalign
                                                 reference index file
            *     --threshold                    Quality threshold to use for the -t
                                                 parameter

        bowtie2SingleEnds      Run a bowtie2 alignment in single ended mode
          Usage: bowtie2SingleEnds [options]
      Options:
            *     --HDFSAlignmentsDir       HDFS directory to hold the alignment
                                            data
            *     --HDFSDataDir             HDFS directory that holds the read data
            *     --HDFSPathToBowtieAlign   HDFS path to the bowtie2 executable
            *     --numReports              Max number of alignment hits to report
                                            with the -k option
            *     --reference               HDFS path to the bowtie 2 fasta
                                            reference file

        gemSingleEnds      Run a GEM alignment
          Usage: gemSingleEnds [options]
      Options:
            *     --HDFSAlignmentsDir     HDFS directory to hold the alignment data
            *     --HDFSDataDir           HDFS directory that holds the read data
            *     --HDFSPathToGEM2SAM     HDFS path to the gem-2-sam executable
            *     --HDFSPathToGEMMapper   HDFS path to the gem-mapper executable
            *     --editDistance          Edit distance parameter (-e) to use in the
                                          GEM mapping
                                          Default: 0
            *     --maxProcessesOnNode    Maximum number of GEM mapping processes to
                                          run on one node simultaneously
                                          Default: 6
            *     --numReports            Max number of hits to report from GEM
            *     --reference             HDFS path to the GEM reference file
                  --strata                Strata parameter (-s) to use in the GEM
                                          mapping
                                          Default: all

        razerS3SingleEnds      Run a razerS3 alignment
          Usage: razerS3SingleEnds [options]
      Options:
            *     --HDFSAlignmentsDir   HDFS directory to hold the alignment data
            *     --HDFSDataDir         HDFS directory that holds the read data
            *     --HDFSPathToRazerS3   HDFS path to the razers3 executable file
            *     --numReports          Max number of alignments to report for each
                                        read
            *     --pctIdentity         RazerS 3 percent identity parameter (-i)
                                        Default: 0
            *     --reference           HDFS path to the reference (FASTA) file for
                                        the RazerS 3 mapper
            *     --sensitivity         RazerS 3 sensitivity parameter (-rr)
                                        Default: 0

        mrfastSingleEnds      Run a novoalign mate pair alignment
          Usage: mrfastSingleEnds [options]
      Options:
            *     --HDFSAlignmentsDir   HDFS directory to hold the alignment data
            *     --HDFSDataDir         HDFS directory that holds the read data
            *     --HDFSPathToMrfast    HDFS path to the mrfast executable file
            *     --reference           HDFS path to the mrfast reference index file
                  --threshold           MrFAST threshold parameter (-e)
                                        Default: -1

        exportAlignmentsFromHDFS      Export alignments in SAM format
          Usage: exportAlignmentsFromHDFS [options]
      Options:
                  --aligner        Format of the alignment records
                                   (sam|mrfast|novoalign)
                                   Default: sam
            *     --inputHDFSDir   HDFS path to the directory holding the alignment
                                   reccords

        GMMFitSingleEndInsertSizes      Compute GMM features in each bin across the genome
          Usage: GMMFitSingleEndInsertSizes [options]
      Options:
                  --aligner                            Format of the alignment
                                                       records (sam|mrfast|novoalign)
                                                       Default: sam
                  --chrFilter                          If filter params are used,
                                                       only consider alignments in the
                                                       region
                                                       chrFilter:startFilter-endFilter
                  --endFilter                          See chrFilter
                  --excludePairsMappingIn              HDFS path to a BED file. Any
                                                       reads mapped within those intervals
                                                       will be excluded from the
                                                       processing
            *     --faidx                              HDFS path to the chromosome
                                                       length file for the reference genome
            *     --inputFileDescriptor                HDFS path to the directory
                                                       that holds the alignment records
                  --legacyAlignments                   Use data generated with an
                                                       older version of Cloudbreak
                                                       Default: false
                  --mapabilityWeighting                HDFS path to a BigWig file
                                                       containing genome uniqness scores. If
                                                       specified, Cloudbreak will weight reads
                                                       by the uniqueness of the regions
                                                       they mapped to
                  --maxInsertSize                      Maximum insert size to
                                                       consider (= max size of deletion
                                                       detectable)
                                                       Default: 25000
                  --maxLogMapqDiff                     Adaptive quality score cutoff
                                                       Default: 5.0
                  --maxMismatches                      Max number of mismatches
                                                       allowed in an alignment; all other
                                                       will be ignored
                                                       Default: -1
                  --minCleanCoverage                   Minimum number of spanning
                                                       read pairs for a bin to run the
                                                       GMM fitting procedure
                                                       Default: 3
                  --minScore                           Minimum alignment score (SAM
                                                       tag AS); all reads with lower AS
                                                       will be ignored
                                                       Default: -1
            *     --outputHDFSDir                      HDFS path to the directory
                                                       that will hold the output of the
                                                       GMM procedure
                  --resolution                         Size of the bins to tile the
                                                       genome with
                                                       Default: 25
                  --startFilter                        See chrFilter
                  --stripChromosomeNamesAtWhitespace   Clip chromosome names from
                                                       the reference at the first
                                                       whitespace so they match with alignment
                                                       fields
                                                       Default: false

        extractDeletionCalls      Extract deletion calls into a BED file
          Usage: extractDeletionCalls [options]
      Options:
            *     --faidx                Chromosome length file for the reference
            *     --inputHDFSDir         HDFS path to the GMM fit feature results
                  --medianFilterWindow   Use a median filter of this size to clean
                                         up the results
                                         Default: 5
            *     --outputHDFSDir        HDFS Directory to store the variant calls
                                         in
                  --resolution           Size of the bins to tile the genome with
                                         Default: 25
            *     --targetIsize          Mean insert size of the library
                                         Default: 0
            *     --targetIsizeSD        Standard deviation of the insert size of
                                         the library
                                         Default: 0
                  --threshold            Likelihood ratio threshold to call a
                                         variant
                                         Default: 1.68

        extractInsertionCalls      Extract insertion calls into a BED file
          Usage: extractInsertionCalls [options]
      Options:
            *     --faidx                Chromosome length file for the reference
            *     --inputHDFSDir         HDFS path to the GMM fit feature results
                  --medianFilterWindow   Use a median filter of this size to clean
                                         up the results
                                         Default: 5
                  --noCovFilter          filter out calls next to a bin with no
                                         coverage - recommend on for BWA alignments, off for
                                         other aligners
                                         Default: true
            *     --outputHDFSDir        HDFS Directory to store the variant calls
                                         in
                  --resolution           Size of the bins to tile the genome with
                                         Default: 25
            *     --targetIsize          Mean insert size of the library
                                         Default: 0
            *     --targetIsizeSD        Standard deviation of the insert size of
                                         the library
                                         Default: 0
                  --threshold            Likelihood ratio threshold to call a
                                         variant
                                         Default: 1.68

        copyToS3      Upload a file to Amazon S3 using multi-part upload
          Usage: copyToS3 [options]
      Options:
            *     --S3Bucket   S3 Bucket to upload to
            *     --fileName   Path to the file to be uploaded on the local
                               filesystem

        launchCluster      Use whirr to create a new cluster in the cloud using whirr/cloudbreak-whirr.properties
          Usage: launchCluster [options]
        runScriptOnCluster      Execute a script on one node of the currently running cloud cluster
          Usage: runScriptOnCluster [options]
      Options:
            *     --fileName   Path on the local filesystem of the script to run

        destroyCluster      Destroy the currently running whirr cluster
          Usage: destroyCluster [options]
        summarizeAlignments      Gather statistics about a set of alignments: number of reads, number of mappings, and total number of mismatches
          Usage: summarizeAlignments [options]
      Options:
                  --aligner        Format of the alignment records
                                   (sam|mrfast|novoalign)
                                   Default: sam
            *     --inputHDFSDir   HDFS path of the directory that holds the
                                   alignments

        exportGMMResults      Export wig files that contain the GMM features across the entire genome
          Usage: exportGMMResults [options]
      Options:
            *     --faidx          Local path to the chromosome length file
            *     --inputHDFSDir   HDFS path to the directory holding the GMM
                                   features
            *     --outputPrefix   Prefix of the names of the files to create
                  --resolution     Bin size that the GMM features were computed for
                                   Default: 25

        dumpReadsWithScores      Dump all read pairs that span the given region with their deletion scores to BED format (debugging)
          Usage: dumpReadsWithScores [options]
      Options:
                  --aligner                            Format of the alignment
                                                       records (sam|mrfast|novoalign)
                                                       Default: sam
            *     --inputFileDescriptor                HDFS path to the directory
                                                       that holds the alignment records
                  --maxInsertSize                      Maximum possible insert size
                                                       to consider
                                                       Default: 500000
                  --minScore                           Minimum alignment score (SAM
                                                       tag AS); all reads with lower AS
                                                       will be ignored
                                                       Default: -1
            *     --outputHDFSDir                      HDFS path to the directory
                                                       that will hold the output
            *     --region                             region to find read pairs
                                                       for, in chr:start-end format
                  --stripChromosomeNamesAtWhitespace   Clip chromosome names from
                                                       the reference at the first
                                                       whitespace so they match with alignment
                                                       fields
                                                       Default: false

        debugReadPairInfo      Compute the raw data that goes into the GMM fit procedure for each bin (use with filter to debug a particular locus)
          Usage: debugReadPairInfo [options]
      Options:
                  --aligner                 Format of the alignment records
                                            (sam|mrfast|novoalign)
                                            Default: sam
            *     --chrFilter               Print info for alignments in the region
                                            chrFilter:startFilter-endFilter
            *     --endFilter               see chrFilter
                  --excludePairsMappingIn   HDFS path to a BED file. Any reads
                                            mapped within those intervals will be excluded
                                            from the processing
            *     --faidx                   HDFS path to the chromosome length file
                                            for the reference genome
            *     --inputFileDescriptor     HDFS path to the directory that holds
                                            the alignment records
                  --mapabilityWeighting     HDFS path to a BigWig file containing
                                            genome uniqness scores. If specified,
                                            Cloudbreak will weight reads by the uniqueness of
                                            the regions they mapped to
                  --maxInsertSize           Maximum insert size to consider (= max
                                            size of deletion detectable)
                                            Default: 500000
                  --minScore                Minimum alignment score (SAM tag AS);
                                            all reads with lower AS will be ignored
                                            Default: -1
            *     --outputHDFSDir           HDFS directory to hold the output
                  --resolution              Size of the bins to tile the genome with
                                            Default: 25
            *     --startFilter             see chrFilter

        findAlignment      Find an alignment record that matches the input string
          Usage: findAlignment [options]
      Options:
            *     --HDFSAlignmentsDir   HDFS path to the directory that stores the
                                        alignment data
            *     --outputHDFSDir       HDFS path to the directory in which to put
                                        the results
            *     --read                Read name or portion of the read name to
                                        search for

        sortGMMResults      Sort and merge GMM Results (use with one reducer to get all GMM feature results into a single file
          Usage: sortGMMResults [options]
      Options:
            *     --inputHDFSDir    HDFS path to the directory holding the GMM
                                    features
            *     --outputHDFSDir   Directory in which to put the results



