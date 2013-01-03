package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.NovoalignNativeRecord;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 8/8/12
 * Time: 4:28 PM
 */
public class NovoalignAlignmentReaderTest {
    @Test
    public void testProbabilityMappingIsCorrect() {
        assertTrue(NovoalignAlignmentReader.probabilityMappingIsCorrect(NovoalignNativeRecord.decodePosterior(185), NovoalignNativeRecord.decodePosterior(117)) > NovoalignAlignmentReader.probabilityMappingIsCorrect(NovoalignNativeRecord.decodePosterior(185), NovoalignNativeRecord.decodePosterior(0)));
    }

}
