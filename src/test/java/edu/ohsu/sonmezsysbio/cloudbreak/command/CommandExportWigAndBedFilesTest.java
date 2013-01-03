package edu.ohsu.sonmezsysbio.cloudbreak.command;

import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.file.DFSFacade;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.StringBufferInputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/10/12
 * Time: 10:00 AM
 */
public class CommandExportWigAndBedFilesTest {
    @Test
    public void testMergeSortedInputStreams() throws Exception {
        String file1 = "chr1\t1000\t-2\n" +
                "chr1\t2000\t-4\n" +
                "chr2\t2000\t-6\n";

        String file2 = "chr1\t3000\t-1\n" +
                "chr2\t1000\t-3\n" +
                "chr2\t3000\t-5\n";

        List<Path> inputStreams = new ArrayList<Path>();
        Path p1 = Mockito.mock(Path.class);
        Path p2 = Mockito.mock(Path.class);
        inputStreams.add(p1);
        inputStreams.add(p2);

        CommandExportWigAndBedFiles command = new CommandExportWigAndBedFiles();

        StringWriter writer = new StringWriter();

        FaidxFileHelper faix = new FaidxFileHelper("foo") {
            @Override
            public String getNameForChromKey(Short key) throws IOException {
                if (key == (short) 0) return "chr1";
                if (key == (short) 1) return "chr2";
                return "foo";
            }

            @Override
            public Short getKeyForChromName(String name) throws IOException {
                if ("chr1".equals(name)) return 0;
                if ("chr2".equals(name)) return 1;
                return -1;
            }
        };

        DFSFacade dfs = Mockito.mock(DFSFacade.class);
        when(dfs.openPath(p1)).thenReturn(new StringBufferInputStream(file1));
        when(dfs.openPath(p2)).thenReturn(new StringBufferInputStream(file2));

        command.mergeSortedInputStreams(dfs, writer, faix, true, inputStreams);

        String expectedOutput = "variableStep chrom=chr1 span=" + Cloudbreak.DEFAULT_RESOLUTION + "\n" +
                "1000\t-2.0\n" +
                "2000\t-4.0\n" +
                "3000\t-1.0\n" +
                "variableStep chrom=chr2 span=" + Cloudbreak.DEFAULT_RESOLUTION + "\n" +
                "1000\t-3.0\n" +
                "2000\t-6.0\n" +
                "3000\t-5.0\n";

        assertEquals(expectedOutput, writer.getBuffer().toString());
    }
}
