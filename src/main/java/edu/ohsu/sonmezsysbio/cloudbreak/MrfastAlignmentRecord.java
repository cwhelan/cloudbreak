package edu.ohsu.sonmezsysbio.cloudbreak;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 2:33 PM
 */
public class MrfastAlignmentRecord implements AlignmentRecord {

    String chromosomeName;
    int position;
    boolean forward;
    String readId;
    int sequenceLength;
    int mismatches;

    public String getChromosomeName() {
        return chromosomeName;
    }

    public void setChromosomeName(String chromosomeName) {
        this.chromosomeName = chromosomeName;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public boolean isForward() {
        return forward;
    }

    public void setForward(boolean forward) {
        this.forward = forward;
    }

    public String getReadId() {
        return readId;
    }

    public void setReadId(String readId) {
        this.readId = readId;
    }

    public boolean isMapped() {
        return true;
    }

    public int getMismatches() {
        return mismatches;
    }

    public void setMismatches(int mismatches) {
        this.mismatches = mismatches;
    }

    public int getAlignmentScore() {
        return getSequenceLength() - getMismatches();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MrfastAlignmentRecord that = (MrfastAlignmentRecord) o;

        if (forward != that.forward) return false;
        if (mismatches != that.mismatches) return false;
        if (position != that.position) return false;
        if (sequenceLength != that.sequenceLength) return false;
        if (chromosomeName != null ? !chromosomeName.equals(that.chromosomeName) : that.chromosomeName != null)
            return false;
        if (readId != null ? !readId.equals(that.readId) : that.readId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = chromosomeName != null ? chromosomeName.hashCode() : 0;
        result = 31 * result + position;
        result = 31 * result + (forward ? 1 : 0);
        result = 31 * result + (readId != null ? readId.hashCode() : 0);
        result = 31 * result + sequenceLength;
        result = 31 * result + mismatches;
        return result;
    }

    public int getSequenceLength() {

        return sequenceLength;
    }

    public void setSequenceLength(int sequenceLength) {
        this.sequenceLength = sequenceLength;
    }

}
