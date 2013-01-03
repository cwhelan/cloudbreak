package edu.ohsu.sonmezsysbio.cloudbreak;

import net.sf.samtools.Cigar;
import net.sf.samtools.TextCigarCodec;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/23/11
 * Time: 10:44 AM
 */
public class SAMRecord implements AlignmentRecord {

    private static org.apache.log4j.Logger log = Logger.getLogger(SAMRecord.class);

    int flag;
    Map<String,String> tags = new HashMap<String, String>();
    String referenceName;
    String pairReferenceName;
    int position;
    int insertSize;
    String readPairId;
    String sequence;
    String cigar;

    public static SAMRecord parseSamRecord(String[] fields) {
        SAMRecord samRecord = new SAMRecord();
        samRecord.readPairId = fields[0];
        samRecord.flag = Integer.parseInt(fields[1]);
        samRecord.referenceName = fields[2];
        samRecord.position = Integer.parseInt(fields[3]);
        samRecord.cigar = fields[5];
        samRecord.sequence = fields[9];
        if (! samRecord.isMapped()) {
            return samRecord;
        }

        samRecord.pairReferenceName = fields[6];
        samRecord.insertSize = Integer.parseInt(fields[8]);

        int optionalFieldsStart = 11;
        for (int i = optionalFieldsStart; i < fields.length; i++) {
            samRecord.addTag(fields[i]);
        }
        return samRecord;
    }

    public int getPosition() {
        return position;
    }

    public int getInsertSize() {
        return insertSize;
    }

    public String getReferenceName() {
        return referenceName;
    }

    public String getPairReferenceName() {
        return pairReferenceName;
    }

    public int getFlag() {
        return flag;
    }

    public void addTag(String tag) {
        String[] tagFields = tag.split(":");
        tags.put(tagFields[0], tagFields[2]);
    }

    public boolean isMapped() {
        return ! ((flag & 0x4) == 0x4);
    }

    public boolean isMateMapped() {
        return ! ((flag & 0x8) == 0x8);
    }

    public int getPairPosterior() {
        return Integer.parseInt(tags.get("PQ"));
    }

    public int getEndPosterior() {
        return Integer.parseInt(tags.get("UQ"));
    }

    public boolean isPairMapped() {
        return isMapped() && isMateMapped();
    }

    public boolean isReverseComplemented() {
        return ((flag & 0x10) == 0x10);
    }

    public boolean isInterChromosomal() {
        return ! "=".equals(pairReferenceName) && ! referenceName.equals(pairReferenceName);
    }

    public boolean isProperPair() {
        return ((flag & 0x2) == 0x2);
    }

    public boolean isAlignmentOfFirstRead() {
        return ((flag & 0x40) == 0x40);
    }

    public String getSequence() {
        return sequence;
    }

    public String getChromosomeName() {
        return getReferenceName();
    }

    public boolean isForward() {
        return ! isReverseComplemented();
    }

    public String getReadId() {
        return readPairId;
    }

    public int getSequenceLength() {
        Cigar cigar = TextCigarCodec.getSingleton().decode(this.cigar);
        return cigar.getReadLength();
    }

    public void setFlag(int i) {
        flag = i;
    }

    public int getAlignmentScore() {
        return Integer.parseInt(tags.get("AS"));
    }

    @Override
    public String toString() {
        return "SAMRecord{" +
                "flag=" + flag +
                ", tags=" + tags +
                ", referenceName='" + referenceName + '\'' +
                ", pairReferenceName='" + pairReferenceName + '\'' +
                ", position=" + position +
                ", insertSize=" + insertSize +
                ", readPairId='" + readPairId + '\'' +
                ", sequence='" + sequence + '\'' +
                ", cigar='" + cigar + '\'' +
                '}';
    }

    public int getMismatches() {
        int nm = 0;
        if (! tags.containsKey("NM")) {
            return 0;
        }
        try {
            nm = Integer.parseInt(tags.get("NM"));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            log.error(e);
            log.error("sam record " + toString());
            throw new BadAlignmentRecordException(e);
        }
        return nm;
    }

}
