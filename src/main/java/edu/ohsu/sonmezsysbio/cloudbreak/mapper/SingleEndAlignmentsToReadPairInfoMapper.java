package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import edu.ohsu.sonmezsysbio.cloudbreak.*;
import edu.ohsu.sonmezsysbio.cloudbreak.file.BigWigFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.file.GFFFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.io.AlignmentRecordFilter;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GenomicLocationWithQuality;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReadPairInfo;
import edu.ohsu.sonmezsysbio.svpipeline.io.GenomicLocation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/6/12
 * Time: 1:03 PM
 */
public class SingleEndAlignmentsToReadPairInfoMapper extends SingleEndAlignmentsMapper
        implements Mapper<Text, Text, GenomicLocationWithQuality, ReadPairInfo>, AlignmentRecordFilter {

    private static org.apache.log4j.Logger logger = Logger.getLogger(SingleEndAlignmentsToReadPairInfoMapper.class);

    { logger.setLevel(Level.INFO); }

    private PairedAlignmentScorer scorer;
    private String faidxFileName;
    FaidxFileHelper faix;

    // for debugging, restrict output to a particular region
    private String chromosomeFilter;
    private Long startFilter;
    private Long endFilter;
    private GFFFileHelper exclusionRegions;
    private BigWigFileHelper mapabilityWeighting;

    private int minScore = -1;
    private int maxMismatches = -1;

    public int getMaxMismatches() {
        return maxMismatches;
    }

    public void setMaxMismatches(int maxMismatches) {
        this.maxMismatches = maxMismatches;
    }

    public int getMinScore() {
        return minScore;
    }

    public void setMinScore(int minScore) {
        this.minScore = minScore;
    }

    public FaidxFileHelper getFaix() {
        return faix;
    }

    public void setFaix(FaidxFileHelper faix) {
        this.faix = faix;
    }

    public String getChromosomeFilter() {
        return chromosomeFilter;
    }

    public void setChromosomeFilter(String chromosomeFilter) {
        this.chromosomeFilter = chromosomeFilter;
    }

    public Long getStartFilter() {
        return startFilter;
    }

    public void setStartFilter(Long startFilter) {
        this.startFilter = startFilter;
    }

    public Long getEndFilter() {
        return endFilter;
    }

    public void setEndFilter(Long endFilter) {
        this.endFilter = endFilter;
    }

    public Integer getMaxInsertSize() {
        return maxInsertSize;
    }

    public void setMaxInsertSize(Integer maxInsertSize) {
        this.maxInsertSize = maxInsertSize;
    }

    public PairedAlignmentScorer getScorer() {
        return scorer;
    }

    public void setScorer(PairedAlignmentScorer scorer) {
        this.scorer = scorer;
    }

    public GFFFileHelper getExclusionRegions() {
        return exclusionRegions;
    }

    public void setExclusionRegions(GFFFileHelper exclusionRegions) {
        this.exclusionRegions = exclusionRegions;
    }

    public void map(Text key, Text value, OutputCollector<GenomicLocationWithQuality, ReadPairInfo> output, Reporter reporter) throws IOException {
        String line = value.toString();

        ReadPairAlignments readPairAlignments = alignmentReader.parsePairAlignmentLine(line, this);

        // ignoring OEA for now
        if (readPairAlignments.getRead1Alignments().size() == 0 || readPairAlignments.getRead2Alignments().size() == 0) {
            return;
        }

        Set<AlignmentRecord> recordsInExcludedAreas = new HashSet<AlignmentRecord>();
        try {
            if (exclusionRegions != null) {
                for (AlignmentRecord record : readPairAlignments.getRead1Alignments()) {
                    if (exclusionRegions.doesLocationOverlap(record.getChromosomeName(), record.getPosition(), record.getPosition() + record.getSequenceLength())) {
                        logger.debug("excluding record " + record);
                        recordsInExcludedAreas.add(record);
                    }
                }

                for (AlignmentRecord record : readPairAlignments.getRead2Alignments()) {
                    if (exclusionRegions.doesLocationOverlap(record.getChromosomeName(), record.getPosition(), record.getPosition() + record.getSequenceLength())) {
                        logger.debug("excluding record " + record);
                        recordsInExcludedAreas.add(record);
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        try {
            Map<GenomicLocation, ReadPairInfo> bestScoresForGL = emitReadPairInfoForAllPairs(readPairAlignments, output, recordsInExcludedAreas);
            for (GenomicLocation genomicLocation : bestScoresForGL.keySet()) {
                ReadPairInfo bestRpi = bestScoresForGL.get(genomicLocation);
                GenomicLocationWithQuality genomicLocationWithQuality =
                        new GenomicLocationWithQuality(genomicLocation.chromosome, genomicLocation.pos, bestRpi.pMappingCorrect);
                output.collect(genomicLocationWithQuality, bestRpi);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Map<GenomicLocation, ReadPairInfo> emitReadPairInfoForAllPairs(ReadPairAlignments readPairAlignments,
                                             OutputCollector<GenomicLocationWithQuality, ReadPairInfo> output,
                                             Set<AlignmentRecord> recordsInExcludedAreas) throws Exception {
        Map<GenomicLocation, ReadPairInfo> bestScoresForGL = new HashMap<GenomicLocation, ReadPairInfo>();
        if (!emitConcordantAlignmentIfFound(readPairAlignments, output, bestScoresForGL)) {

            for (String chrom : readPairAlignments.getRead1AlignmentsByChromosome().keySet()) {
                if (! readPairAlignments.getRead2AlignmentsByChromosome().containsKey(chrom))
                    continue;

                if (getChromosomeFilter() != null && ! chrom.equals(getChromosomeFilter()))
                    continue;

                for (AlignmentRecord record1 : readPairAlignments.getRead1AlignmentsByChromosome().get(chrom)) {
                    if (getMinScore() != -1) {
                        if (record1.getAlignmentScore() < getMinScore()) {
                            continue;
                        }
                    }
                    for (AlignmentRecord record2 : readPairAlignments.getRead2AlignmentsByChromosome().get(chrom)) {
                        if (getMinScore() != -1) {
                            if (record2.getAlignmentScore() < getMinScore()) {
                                continue;
                            }
                        }
                        if (recordsInExcludedAreas.contains(record1) || recordsInExcludedAreas.contains(record2)) continue;
                        emitReadPairInfoForPair(record1, record2, readPairAlignments, output, bestScoresForGL);
                    }
                }
            }
        }
        return bestScoresForGL;
    }

    private boolean emitConcordantAlignmentIfFound(ReadPairAlignments readPairAlignments,
                                                   OutputCollector<GenomicLocationWithQuality, ReadPairInfo> output,
                                                   Map<GenomicLocation, ReadPairInfo> bestScoresForGL) throws IOException {
        boolean foundConcordant = false;
        for (String chrom : readPairAlignments.getRead1AlignmentsByChromosome().keySet()) {
            if (getChromosomeFilter() != null && ! chrom.equals(getChromosomeFilter()))
                continue;

            if (! readPairAlignments.getRead2AlignmentsByChromosome().containsKey(chrom))
                continue;

            for (AlignmentRecord record1 : readPairAlignments.getRead1AlignmentsByChromosome().get(chrom)) {
                for (AlignmentRecord record2 : readPairAlignments.getRead2AlignmentsByChromosome().get(chrom)) {
                    if (!scorer.validateMappingOrientations(record1, record2, isMatePairs())) continue;
                    AlignmentRecord leftRead = record1.getPosition() < record2.getPosition() ?
                            record1 : record2;
                    AlignmentRecord rightRead = record1.getPosition() < record2.getPosition() ?
                            record2 : record1;

                    int insertSize = rightRead.getPosition() + rightRead.getSequenceLength() - leftRead.getPosition();
                    if (Math.abs(insertSize - getTargetIsize()) < 3 * getTargetIsizeSD()) {
                        emitReadPairInfoForPair(record1, record2, readPairAlignments, output, bestScoresForGL);
                        foundConcordant = true;
                    }
                }
            }
        }
        return foundConcordant;
    }

    private void emitReadPairInfoForPair(AlignmentRecord record1, AlignmentRecord record2, ReadPairAlignments readPairAlignments,
                                         OutputCollector<GenomicLocationWithQuality, ReadPairInfo> output,
                                         Map<GenomicLocation, ReadPairInfo> bestScoresForGL) throws IOException {

        try {
            // todo: not handling translocations for now
            if (! record1.getChromosomeName().equals(record2.getChromosomeName())) {
                if (logger.isDebugEnabled()) {
                    logger.debug("translocation: r1 = " + record1 + "; r2 = " + record2);
                }
                return;
            }

            if (getChromosomeFilter() != null) {
                if (! record1.getChromosomeName().equals(getChromosomeFilter())) {
                    return;
                }
            }

            int insertSize;
            AlignmentRecord leftRead = record1.getPosition() < record2.getPosition() ?
                    record1 : record2;
            AlignmentRecord rightRead = record1.getPosition() < record2.getPosition() ?
                    record2 : record1;

            // todo: not handling inversions for now
            if (!scorer.validateMappingOrientations(record1, record2, isMatePairs())) {
                if (logger.isDebugEnabled()) {
                    logger.debug("failed mapping orientation check: r1 = " + record1 + "; r2 = " + record2 + ", matepair = " + isMatePairs());
                }
                return;
            }

            insertSize = rightRead.getPosition() + rightRead.getSequenceLength() - leftRead.getPosition();

            if (! scorer.validateInsertSize(insertSize, record1.getReadId(), maxInsertSize)) {
                return;
            }

            int leftReadEnd = leftRead.getPosition() + leftRead.getSequenceLength();
            int genomeOffset = leftReadEnd - leftReadEnd % resolution;


            int internalIsize = rightRead.getPosition() - leftReadEnd;
            int genomicWindow = internalIsize +
                    leftReadEnd % resolution +
                    (resolution - rightRead.getPosition() % resolution);


            double pMappingCorrect = alignmentReader.probabilityMappingIsCorrect(record1, record2, readPairAlignments);

            if (mapabilityWeighting != null) {
                if (insertSize > getTargetIsize() + 6 * getTargetIsizeSD()) {
                    String chrom = record1.getChromosomeName();
                    int leftReadStart = leftRead.getPosition();
                    double leftReadMapability = mapabilityWeighting.getMinValueForRegion(chrom, leftReadStart, leftReadEnd);
                    logger.debug("left read mapability from " + leftRead.getPosition() + " to " + leftReadEnd + " = " + leftReadMapability);

                    int rightReadStart = rightRead.getPosition() - rightRead.getSequenceLength();
                    int rightReadEnd = rightRead.getPosition();
                    double rightReadMapability = mapabilityWeighting.getMinValueForRegion(chrom, rightReadStart, rightReadEnd);
                    logger.debug("right read mapability from " + (rightRead.getPosition() - rightRead.getSequenceLength()) + " to " + rightRead.getPosition() + " = " + rightReadMapability);

                    logger.debug("old pmc: " + pMappingCorrect);
                    pMappingCorrect = pMappingCorrect + Math.log(leftReadMapability) + Math.log(rightReadMapability);
                    logger.debug("new pmc: " + pMappingCorrect);
                }
            }

            ReadPairInfo readPairInfo = new ReadPairInfo(insertSize, pMappingCorrect, getReadGroupId());

            for (int i = 0; i <= genomicWindow; i += resolution) {
                Short chromosome = faix.getKeyForChromName(record1.getChromosomeName());
                if (chromosome == null) {
                    throw new RuntimeException("Bad chromosome in record: " + record1.getChromosomeName());
                }

                int pos = genomeOffset + i;

                if (getChromosomeFilter() != null) {
                    if (! record1.getChromosomeName().equals(getChromosomeFilter()) ||
                        pos < getStartFilter() || pos > getEndFilter()) {
                        continue;
                    }
                }

                logger.debug("Emitting insert size " + insertSize);
                GenomicLocationWithQuality genomicLocationWithQuality = new GenomicLocationWithQuality(chromosome, pos, readPairInfo.pMappingCorrect);
                GenomicLocation genomicLocation = new GenomicLocation(genomicLocationWithQuality.chromosome, genomicLocationWithQuality.pos);
                if (bestScoresForGL.containsKey(genomicLocation) && bestScoresForGL.get(genomicLocation).pMappingCorrect >= readPairInfo.pMappingCorrect) {
                    continue;
                } else {
                    bestScoresForGL.put(genomicLocation, readPairInfo);
                }
            }
        } catch (BadAlignmentRecordException e) {
            logger.error(e);
            logger.error("skipping bad record pair: "+ record1 + ", " + record2);
        }

    }


    public void configure(JobConf job) {
        super.configure(job);
        configureReadGroups(job);

        scorer = new ProbabilisticPairedAlignmentScorer();

        faidxFileName = job.get("alignment.faidx");
        faix = new FaidxFileHelper(faidxFileName);

        if (job.get("alignments.filterchr") != null) {
            setChromosomeFilter(job.get("alignments.filterchr"));
            setStartFilter(Long.parseLong(job.get("alignments.filterstart")));
            setEndFilter(Long.parseLong(job.get("alignments.filterend")));
            logger.debug("Configured filter");
        }


        if (job.get("alignment.exclusionRegions") != null) {
            String exclusionRegionsFileName = job.get("alignment.exclusionRegions");
            try {
                exclusionRegions = new GFFFileHelper(exclusionRegionsFileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.debug("configured exclusion regions with " + exclusionRegionsFileName);
        }


        if (job.get("alignment.mapabilityWeighting") != null) {
            String mapabilityWeightingFileName = job.get("alignment.mapabilityWeighting");
            mapabilityWeighting = new BigWigFileHelper();
            try {
                mapabilityWeighting.open(mapabilityWeightingFileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.debug("configured mapability with " + mapabilityWeightingFileName);
        }

        if (job.get("pileupDeletionScore.maxInsertSize") != null) {
            maxInsertSize = Integer.parseInt(job.get("pileupDeletionScore.maxInsertSize"));
            logger.debug("configured max insert to " + maxInsertSize);
        }
        minScore = Integer.parseInt(job.get("pileupDeletionScore.minScore"));

        maxMismatches = Integer.parseInt(job.get("pileupDeletionScore.maxMismatches"));

        logger.debug("done with configuration");
    }

    public boolean passes(AlignmentRecord record) {
        if (maxMismatches != -1 && getAlignerName().equals(Cloudbreak.ALIGNER_GENERIC_SAM)) {
            return ((SAMRecord) record).getMismatches() < maxMismatches;
        }
        return true;
    }
}
