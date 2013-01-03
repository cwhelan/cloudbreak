package edu.ohsu.sonmezsysbio.cloudbreak.file;

import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/9/12
 * Time: 10:06 AM
 */
public class ReadGroupInfoFileHelperTest {

    public static final String READ_GROUP_INFO_FILE =
            "READGROUP1\tLIBRARY1\t200\t30\tfalse\t/path/in/hdfs/to/rg1\n" +
            "READGROUP2\tLIBRARY1\t200\t30\tfalse\t/path/in/hdfs/to/rg2\n" +
            "READGROUP3\tLIBRARY2\t300\t50\tfalse\t/path/in/hdfs/to/rg3\n" +
            "READGROUP4\tLIBRARY3\t2000\t300\ttrue\t/path/in/hdfs/to/rg4\n";

    @Test
    public void testReadReadGroupIdsByHDFSPath() throws IOException {
        ReadGroupInfoFileHelper readGroupInfoFileHelper = new ReadGroupInfoFileHelper();
        Map readGroups = readGroupInfoFileHelper.readReadGroupIdsByHDFSPath(new BufferedReader(new StringReader(READ_GROUP_INFO_FILE)));
        assertEquals((short) 0, readGroups.get("/path/in/hdfs/to/rg1"));
        assertEquals((short) 1, readGroups.get("/path/in/hdfs/to/rg2"));
        assertEquals((short) 2, readGroups.get("/path/in/hdfs/to/rg3"));
        assertEquals((short) 3, readGroups.get("/path/in/hdfs/to/rg4"));
    }

    @Test
    public void testReadReadGroupsById() throws IOException {
        ReadGroupInfoFileHelper readGroupInfoFileHelper = new ReadGroupInfoFileHelper();
        Map<Short,ReadGroupInfo> readGroups = readGroupInfoFileHelper.readReadGroupsById(new BufferedReader(new StringReader(READ_GROUP_INFO_FILE)));
        ReadGroupInfo rg = readGroups.get((short) 1);
        assertEquals("READGROUP2", rg.readGroupName);
        assertEquals("LIBRARY1", rg.libraryName);
        assertEquals(200, rg.isize);
        assertEquals(30, rg.isizeSD);
        assertFalse(rg.matePair);
        assertEquals("/path/in/hdfs/to/rg2", rg.hdfsPath);

        rg = readGroups.get((short) 3);
        assertEquals("READGROUP4", rg.readGroupName);
        assertEquals("LIBRARY3", rg.libraryName);
        assertEquals(2000, rg.isize);
        assertEquals(300, rg.isizeSD);
        assertTrue(rg.matePair);
        assertEquals("/path/in/hdfs/to/rg4", rg.hdfsPath);
    }

}
