package edu.ohsu.sonmezsysbio.cloudbreak.file;

import org.biojava3.genome.parsers.gff.*;

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

    public boolean doesLocationOverlap(String chrom, int start, int end) throws Exception {
        Location query = new Location(start, end);
        if (! featuresByChrom.containsKey(chrom)) return false;
        return featuresByChrom.get(chrom).selectOverlapping(chrom, query, true).size() > 0;
    }
}
