package edu.ohsu.sonmezsysbio.cloudbreak.file;

import org.biojava3.genome.parsers.gff.FeatureI;
import org.biojava3.genome.parsers.gff.FeatureList;
import org.biojava3.genome.parsers.gff.GFF3Reader;
import org.biojava3.genome.parsers.gff.Location;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/19/12
 * Time: 1:07 PM
 */

/**
 * Read and parse a GFF file.
 */
public class GFFFileHelper {

    Map<String, FeatureList> featuresByChrom = new HashMap<String, FeatureList>();

    public GFFFileHelper() {

    }

    public GFFFileHelper(String filename) throws IOException {
        FeatureList features = GFF3Reader.read(filename);
        Iterator<FeatureI> iterator = features.iterator();
        while (iterator.hasNext()) {
            FeatureI feature = iterator.next();
            String chrom = feature.seqname();
            if (! featuresByChrom.containsKey(chrom)) {
                featuresByChrom.put(chrom, new FeatureList());
            }
            FeatureList featureList = featuresByChrom.get(chrom);
            featureList.add(feature);
        }
    }

    /**
     * return true if the given region overlaps a feature in the GFF file. Should be made more efficient with an interval tree.
     * @param chrom
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    public boolean doesLocationOverlap(String chrom, int start, int end) throws Exception {
        Location query = new Location(start, end);
        if (! featuresByChrom.containsKey(chrom)) return false;
        return featuresByChrom.get(chrom).selectOverlapping(chrom, query, true).size() > 0;
    }
}
