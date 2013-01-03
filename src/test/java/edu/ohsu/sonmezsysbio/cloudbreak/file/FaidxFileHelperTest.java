package edu.ohsu.sonmezsysbio.cloudbreak.file;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 4/9/12
 * Time: 4:13 PM
 */
public class FaidxFileHelperTest {

    public static final String FAIDX = "1       249250621       52      60      61\n" +
            "2       243199373       253404903       60      61\n" +
            "3       198022430       500657651       60      61\n" +
            "4       191154276       701980507       60      61\n" +
            "5       180915260       896320740       60      61\n" +
            "6       171115067       1080251307      60      61\n" +
            "7       159138663       1254218344      60      61\n" +
            "8       146364022       1416009371      60      61\n" +
            "9       141213431       1564812846      60      61\n" +
            "10      135534747       1708379889      60      61\n" +
            "11      135006516       1846173603      60      61\n" +
            "12      133851895       1983430282      60      61\n" +
            "13      115169878       2119513096      60      61\n" +
            "14      107349540       2236602526      60      61\n" +
            "15      102531392       2345741279      60      61\n" +
            "16      90354753        2449981581      60      61\n" +
            "17      81195210        2541842300      60      61\n" +
            "18      78077248        2624390817      60      61\n" +
            "19      59128983        2703769406      60      61\n" +
            "20      63025520        2763883926      60      61\n" +
            "21      48129895        2827959925      60      61\n" +
            "22      51304566        2876892038      60      61\n" +
            "X       155270560       2929051733      60      61\n" +
            "Y       59373566        3086910193      60      61\n" +
            "MT      16569   3147273397      70      71\n" +
            "GL000234.1      40531   3147923585      60      61\n";

    @Test
    public void testReadChromKeysByName() throws IOException {
        FaidxFileHelper faidxFileHelper = new FaidxFileHelper("foo");
        Map chromTable = faidxFileHelper.readChromKeysByName(new BufferedReader(new StringReader(FAIDX)));
        assertEquals((short) 1, chromTable.get("2"));
        assertEquals((short) 23, chromTable.get("Y"));
        assertEquals((short) 25, chromTable.get("GL000234.1"));
    }

    @Test
    public void testReadChromLengthsByName() throws IOException {
        FaidxFileHelper faidxFileHelper = new FaidxFileHelper("foo");
        Map chromTable = faidxFileHelper.readChromLengthsByName(new BufferedReader(new StringReader(FAIDX)));
        assertEquals(135006516l, chromTable.get("11"));
    }

}
