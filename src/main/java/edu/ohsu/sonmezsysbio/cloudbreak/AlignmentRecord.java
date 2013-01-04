package edu.ohsu.sonmezsysbio.cloudbreak;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 1:46 PM
 */

/**
 * This is a generic interface for an Alignment record. Could be backed, for example, by a SAMRecord that knows how to parse SAM format.
 */
public interface AlignmentRecord {
    boolean isMapped();

    String getChromosomeName();

    int getPosition();

    boolean isForward();

    String getReadId();

    int getSequenceLength();

    int getAlignmentScore();
}
