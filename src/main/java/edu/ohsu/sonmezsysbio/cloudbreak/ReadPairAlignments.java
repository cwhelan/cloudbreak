package edu.ohsu.sonmezsysbio.cloudbreak;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 3:18 PM
 */

/**
 * Container class for holding a list of alignments for each read in a read pair.
 */
public class ReadPairAlignments {

    Map<String, List<AlignmentRecord>> read1AlignmentsByChromosome;
    Map<String, List<AlignmentRecord>> read2AlignmentsByChromosome;

    List<AlignmentRecord> read1Alignments;
    List<AlignmentRecord> read2Alignments;
    private Double sumMismatchScores1;
    private Double sumMismatchScores2;

    public ReadPairAlignments(List<AlignmentRecord> read1Alignments, List<AlignmentRecord> read2Alignments) {
        this.read1Alignments = read1Alignments;
        this.read2Alignments = read2Alignments;

        read1AlignmentsByChromosome = new HashMap<String, List<AlignmentRecord>>();
        read2AlignmentsByChromosome = new HashMap<String, List<AlignmentRecord>>();

        for (AlignmentRecord record : read1Alignments) {
            insertIntoMap(read1AlignmentsByChromosome, record);
        }

        for (AlignmentRecord record : read2Alignments) {
            insertIntoMap(read2AlignmentsByChromosome, record);
        }
    }

    private void insertIntoMap(Map<String, List<AlignmentRecord>> alignmentByChromMap, AlignmentRecord record) {
        String chrom = record.getChromosomeName();
        if (! alignmentByChromMap.containsKey(chrom)) {
            alignmentByChromMap.put(chrom, new ArrayList<AlignmentRecord>());
        }
        alignmentByChromMap.get(chrom).add(record);
    }

    public List<AlignmentRecord> getRead1Alignments() {
        return read1Alignments;
    }

    public List<AlignmentRecord> getRead2Alignments() {
        return read2Alignments;
    }

    public Map<String, List<AlignmentRecord>> getRead1AlignmentsByChromosome() {
        return read1AlignmentsByChromosome;
    }

    public Map<String, List<AlignmentRecord>> getRead2AlignmentsByChromosome() {
        return read2AlignmentsByChromosome;
    }

    public Double sumMismatchScores1() {
        if (sumMismatchScores1 == null)
            sumMismatchScores1 = sumMismatchScores(read1Alignments);
        return sumMismatchScores1;
    }

    public Double sumMismatchScores2() {
        if (sumMismatchScores2 == null)
            sumMismatchScores2 = sumMismatchScores(read2Alignments);
        return sumMismatchScores2;
    }

    private double sumMismatchScores(List<AlignmentRecord> alignments) {
        double sumAlignmentScores = 0;
        for (AlignmentRecord record : alignments) {
            sumAlignmentScores += record.mismatchScore();
        }
        return sumAlignmentScores;
    }

}

