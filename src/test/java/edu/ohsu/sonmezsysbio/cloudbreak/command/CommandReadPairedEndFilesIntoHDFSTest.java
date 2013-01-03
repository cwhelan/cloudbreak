package edu.ohsu.sonmezsysbio.cloudbreak.command;

import org.junit.Test;
import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/22/12
 * Time: 1:48 PM
 */
public class CommandReadPairedEndFilesIntoHDFSTest {

    @Test
    public void testGetLongestCommonPrefixCasava18Style() throws Exception {
        String s1 = "@EAS139_44:4:1:2:5501";
        String s2 = "@EAS139_44:4:1:2:5502";
        assertEquals("@EAS139_44:4:1:2:550", CommandReadPairedEndFilesIntoHDFS.greatestCommonPrefix(s1, s2));
    }

    @Test
    public void testGetLongestCommonPrefixOldSkoolFastq() throws Exception {
        String s1 = "@2_62477956_62478206_0_1_0_0_2:0:0_3:0:0_0/1";
        String s2 = "@2_62477956_62478206_0_1_0_0_2:0:0_3:0:0_0/2";
        assertEquals("@2_62477956_62478206_0_1_0_0_2:0:0_3:0:0_0", CommandReadPairedEndFilesIntoHDFS.greatestCommonPrefix(s1, s2));
    }

    @Test
    public void testTrigramEntropy() throws Exception {
        String s1 = "CATATACGTGGATACATATATGTGTATTGCATATACATATACGTGGATACATATATGTGTATTGCATATACATATACGTGGATACATATATGTGTCTTGC";
        CommandReadPairedEndFilesIntoHDFS command = new CommandReadPairedEndFilesIntoHDFS();
        double entropy = command.trigramEntropy(s1);
        assertEquals(2.16, entropy, 0.0001);

        String s2 = "TCTACGTGGATACATATATGTGTATTGCATATACATATACGTGGATACATATATGTGTATTGCATATACATCTACGTGGATACATATATTTCTATTGCAT";
        entropy = command.trigramEntropy(s1);
        assertEquals(2.16, entropy, 0.0001);
    }
}
