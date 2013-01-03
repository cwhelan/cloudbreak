package edu.ohsu.sonmezsysbio.cloudbreak;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/18/11
 * Time: 2:12 PM
 */
public class CloudbreakTest {
    @Test
    public void testBuildJCommander() throws Exception {
        String[] argv = { "alignSingleEnds" };
        JCommander jc = Cloudbreak.buildJCommander();
        try {
            jc.parse(argv);
            fail("validation should have failed because no files provided");
        } catch (ParameterException e) {
        }

        String[] argv2 = { "alignSingleEnds",
                "--HDFSDataDir", "/user/whelanch/tmp/svpipelinetest",
                "--HDFSAlignmentsDir", "s_1_1_sequence.txt",
                "--reference", "ref",
                "--threshold", "150",
                "--HDFSPathToNovoalign", "/user/whelanch/executables/novoalign"};
        jc.parse(argv2);

    }
}
