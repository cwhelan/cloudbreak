package edu.ohsu.sonmezsysbio.cloudbreak;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 1:46 PM
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
