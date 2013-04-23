## cloudbreak

Cloudbreak is a Hadoop-based structural variation (SV) caller for Illumina
paired-end DNA sequencing data. Currently Cloudbreak calls genomic insertions
and deletions; we are working on adding support for other types of SVs.

Cloudbreak contains a full pipeline for aligning your data in the form of FASTQ
files using alignment pipelines that generate many possible mappings for every
read, in the Hadoop framework. It then contains Hadoop jobs for computing
genomic features from the alignments, and for calling insertion and deletion
variants from those features.

Currently this documentation is a little sparse; we are working on expanding it
and adding more user-friendly wrapper scripts.

# BUILDING FROM SOURCE

To build the latest version of Cloudbreak, clone the gitub repository. Enter
the top level directory of the repository and enter the command:

`mvn package`

This should compile the code, execute tests, and create the final jar file
in the `target/` directory.

#DEPENDENCIES

Cloudbreak requires a cluster Hadoop 0.20.2 or Cloudera CDH3 to run (the older
mapreduce API). If you don't have a Hadoop cluster, Cloudbreak can also use the
Apache Whirr API to automatically provision a cluster on the Amazon Elastic
Compute Cloud (EC2). See the section on using WHIRR below.

If you wish to run alignments using Cloudbreak, you will need one of the following
aligners:

* GEM (Recommended): http://algorithms.cnag.cat/wiki/The_GEM_library
* RazerS 3: http://www.seqan.de/projects/razers/
* Bowtie2: http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
* Novoalign: http://www.novocraft.com

To install aligner dependencies for use by Cloudbreak, first generate the index
for the genome reference you would like to run against. Then, copy all of the
required index files, and the executable files for the aligner into HDFS using
the `hdfs -copyFromLocal` command. For GEM you will need the index file created
by running `gem-index`. You will also need an 'fai' file for the reference,
containing chromosome names and lengths, generated by `samtools faidx`.

If your reference file is `reference.fa`, and you've created the files
`reference.gem` and `reference.fa.fai` as described above, issue the following
commands to load the necessary files into HDFS:

    hdfs -mkdir indices/
    hdfs -mkdir executables/
    hdfs -copyFromLocal reference.gem indices/
    hdfs -copyFromLocal reference.fa.fai indices/
    hdfs -copyFromLocal /path/to/gem/executables/gem-mapper executables/
    hdfs -copyFromLocal /path/to/gem/executables/gem-2-sam executables/

# RUNNING CLOUDBREAK

We have created a script to run through the full process of executing the
Cloudbreak pipeline from FASTQ files to insertion and deletion calls. The
script is named `Cloudbreak-full.sh` and can be found in the scripts directory
of the Cloudbreak distribution. To customize the script for your needs, copy
it to a new location and edit the variables in the first three sections:
"EXPERIMENT DETAILS", "LOCAL FILES AND DIRECTORIES", and
"HDFS FILES AND DIRECTORIES".

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

#INDIVIDUAL COMMANDS

All of Cloudbreak's functionality is contained in the executable jar file in the
directory where you unpacked the Cloudbreak distribution. Use the 'hadoop'
command to run the jar file to ensure that the necessary Hadoop dependencies
are available to Cloudbreak.

The usage of each command is detailed below; you can view this information by typing
'hadoop jar cloudbreak-${project.version}.jar' without any additional parameters:

Usage: Cloudbreak [options] [command] [command options]
  Options:
    --help, -help, -?, -h   Help
                            Default: false
  Commands:
    readPairedEndFilesIntoHDFS      Load paired fastq files into HDFS
      Usage: readPairedEndFilesIntoHDFS [options]      
  Options:
        *     --HDFSDataDir               
              --clipReadIdsAtWhitespace   
                                          Default: true
              --compress                  
                                          Default: snappy
        *     --fastqFile1                
        *     --fastqFile2                
              --outFileName               
                                          Default: reads
              --trigramEntropyFilter      
                                          Default: -1.0

    readSAMFileIntoHDFS      Load paired fastq files into HDFS
      Usage: readSAMFileIntoHDFS [options]      
  Options:
        *     --HDFSDataDir   
              --compress      
                              Default: none
              --outFileName   
                              Default: reads.txt
        *     --samFile       

    alignSingleEnds      Run a novoalign mate pair alignment
      Usage: alignSingleEnds [options]      
  Options:
        *     --HDFSAlignmentsDir            
        *     --HDFSDataDir                  
        *     --HDFSPathToNovoalign          
              --HDFSPathToNovoalignLicense   
              --qualityFormat                
                                             Default: ILMFQ
        *     --reference                    
        *     --threshold                    

    bowtie2SingleEnds      Run a bowtie2 alignment
      Usage: bowtie2SingleEnds [options]      
  Options:
        *     --HDFSAlignmentsDir       
        *     --HDFSDataDir             
        *     --HDFSPathToBowtieAlign   
        *     --numReports              
        *     --reference               

    razerS3SingleEnds      Run a razerS3 alignment
      Usage: razerS3SingleEnds [options]      
  Options:
        *     --HDFSAlignmentsDir   
        *     --HDFSDataDir         
        *     --HDFSPathToRazerS3   
        *     --numReports          
        *     --pctIdentity         
                                    Default: 0
        *     --reference           
        *     --sensitivity         
                                    Default: 0

    mrfastSingleEnds      Run a novoalign mate pair alignment
      Usage: mrfastSingleEnds [options]      
  Options:
        *     --HDFSAlignmentsDir   
        *     --HDFSDataDir         
        *     --HDFSPathToMrfast    
        *     --reference           
              --threshold           
                                    Default: -1

    GMMFitSingleEndInsertSizes      Calculate Deletion Scores Across the Genome via Incremental Belief Update
      Usage: GMMFitSingleEndInsertSizes [options]      
  Options:
              --aligner                 
                                        Default: sam
              --chrFilter               
              --endFilter               
              --excludePairsMappingIn   
        *     --faidx                   
        *     --inputFileDescriptor     
              --mapabilityWeighting     
              --maxInsertSize           
                                        Default: 25000
              --maxLogMapqDiff          
                                        Default: 5.0
              --maxMismatches           
                                        Default: -1
              --minCleanCoverage        
                                        Default: 3
              --minScore                
                                        Default: -1
        *     --outputHDFSDir           
              --resolution              
                                        Default: 25
              --startFilter             

    exportWigAndBedFiles      Export Wig files and Bed file of deletions
      Usage: exportWigAndBedFiles [options]      
  Options:
        *     --faidx                
        *     --inputHDFSDir         
              --medianFilterWindow   
                                     Default: 1
        *     --outputPrefix         
              --resolution           
                                     Default: 25
              --text                 
                                     Default: false

    exportGMMResults      Export Wig files and Bed file of deletions
      Usage: exportGMMResults [options]      
  Options:
        *     --faidx          
        *     --inputHDFSDir   
        *     --outputPrefix   
              --resolution     
                               Default: 25

    dumpReadsWithScores      Dump all spanning read pairs with their deletion scores to BED format (debugging)
      Usage: dumpReadsWithScores [options]      
  Options:
              --aligner               
                                      Default: sam
        *     --inputFileDescriptor   
              --isMatePairs           
                                      Default: false
              --maxInsertSize         
                                      Default: 500000
              --minScore              
                                      Default: -1
        *     --outputHDFSDir         
        *     --region                
        *     --targetIsize           
                                      Default: 0
        *     --targetIsizeSD         
                                      Default: 0

    extractPositiveRegionsFromWig      Extract positive regions from a WIG file into a BED file
      Usage: extractPositiveRegionsFromWig [options]      
  Options:
              --extraWigFilesToAverage   
                                         Default: []
        *     --faidx                    
        *     --inputWigFile             
              --medianFilterWindow       
                                         Default: 1
              --muFile                   
        *     --name                     
        *     --outputBedFile            
              --threshold                
                                         Default: 0.0

    debugReadPairInfo      View read pair infos
      Usage: debugReadPairInfo [options]      
  Options:
              --aligner                 
                                        Default: sam
        *     --chrFilter               
        *     --endFilter               
              --excludePairsMappingIn   
        *     --faidx                   
        *     --inputFileDescriptor     
              --mapabilityWeighting     
              --maxInsertSize           
                                        Default: 500000
              --minScore                
                                        Default: -1
        *     --outputHDFSDir           
              --resolution              
                                        Default: 25
        *     --startFilter             

    findAlignment      Find an alignment record that matches the input string
      Usage: findAlignment [options]      
  Options:
        *     --HDFSAlignmentsDir   
        *     --outputHDFSDir       
        *     --read                

    summarizeAlignments      Usage: summarizeAlignments [options]      
  Options:
              --aligner         
                                Default: sam
        *     --inputHDFSDir    
        *     --outputHDFSDir   

    findGenomicLocationsOverThreshold      Usage: findGenomicLocationsOverThreshold [options]      
  Options:
        *     --inputHDFSDir    
        *     --outputHDFSDir   
        *     --threshold       

    exportAlignmentsFromHDFS      Export alignments in SAM format
      Usage: exportAlignmentsFromHDFS [options]      
  Options:
        *     --inputHDFSDir   

    sortGMMResults      Sort and merge GMM Results
      Usage: sortGMMResults [options]      
  Options:
        *     --inputHDFSDir    
        *     --outputHDFSDir   

