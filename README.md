cloudbreak
==========

Cloudbreak is a Hadoop-based structural variation (SV) caller for Illumina paired-end DNA sequencing data. Currently Cloudbreak only
calls genomic deletions; we are working on adding support for other types of SVs.

Cloudbreak contains a full pipeline for aligning your data in the form of FASTQ files using alignment pipelines
that generate all or most possible mappings for every read, in the Hadoop framework. It then contains Hadoop
jobs for computing genomic features from the alignments, and a post-processing step for extracting the features
from HDFS and calling SVs.

Currently this documentation is a little sparse; we are working on expanding it and adding user-friendly wrapper scripts, as well
as Apache Whirr configurations to allow automatic deployment on a cloud provider.

DEPENDENCIES

Cloudbreak requires a cluster Hadoop 0.0.20 or Cloudera CDH3 to run (the older mapreduce API). If you wish to run alignments
using Cloudbreak, you will need one of the following aligners:

RazerS 3 (Recommended): http://www.seqan.de/projects/razers/
Bowtie2: http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
Novoalign: http://www.novocraft.com

To install aligner dependencies for use by Cloudbreak, first generate the index for the genome reference you would
like to run against. Then, copy all of the required index files, and the executable files for the aligner into HDFS using
the 'hdfs -copyFromLocal' command.

RUNNING CLOUDBREAK

All of Cloudbreak's functionality is contained in the executable jar file in the lib/ directory where you unpacked the
Cloudbreak distribution. Use the 'hadoop' command to run the jar file to ensure that the necessary Hadoop dependencies
are available to Cloudbreak. The usage of each command is detailed below; you can view this information by typing
'hadoop jar lib/cloudbreak-1.0-exe.jar' without any additional parameters:

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

