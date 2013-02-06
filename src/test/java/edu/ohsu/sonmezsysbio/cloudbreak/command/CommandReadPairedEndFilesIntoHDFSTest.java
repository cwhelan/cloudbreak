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
    }

    @Test
    public void testCasava18QCFilter() throws Exception {
        String read1 = "@HWI-ST632:98:D07DPACXX:1:1101:14035:1998 1:N:0:";
        String read2 = "@HWI-ST632:98:D07DPACXX:1:1101:14035:1998 2:N:0:";
        CommandReadPairedEndFilesIntoHDFS command = new CommandReadPairedEndFilesIntoHDFS();
        assertEquals(true, command.passCasava18QCFilter(read1, read2));

        read1 = "@HWI-ST632:98:D07DPACXX:1:1101:14131:1998 1:Y:0:";
        read2 = "@HWI-ST632:98:D07DPACXX:1:1101:14131:1998 2:N:0:";
        assert(!command.passCasava18QCFilter(read1, read2));
    }
}
