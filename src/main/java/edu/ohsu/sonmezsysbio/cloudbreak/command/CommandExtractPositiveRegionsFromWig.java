package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.file.WigFileHelper;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/22/12
 * Time: 10:33 PM
 */
@Parameters(separators = "=", commandDescription = "Extract positive regions from a WIG file into a BED file")
public class CommandExtractPositiveRegionsFromWig implements CloudbreakCommand {

    @Parameter(names = {"--name"}, required = true)
    String name;
    
    @Parameter(names = {"--inputWigFile"}, required = true)
    String inputWigFile;

    @Parameter(names = {"--outputBedFile"}, required = true)
    String outputBedFile;

    @Parameter(names = {"--faidx"}, required = true)
    private String faidxFileName;

    @Parameter(names = {"--threshold"})
    Double threshold = 0.0;

    @Parameter(names = {"--medianFilterWindow"})
    int medianFilterWindow = 1;

    @Parameter(names = {"--muFile"})
    String muFile = null;

    @Parameter(names = {"--extraWigFilesToAverage"})
    List<String> extraWigFilesToAverage = new ArrayList<String>();

    public void run(Configuration conf) throws Exception {
        FaidxFileHelper faidx = new FaidxFileHelper(faidxFileName);

        BufferedReader wigFileReader = openReader(inputWigFile);

        BufferedReader muFileReader = null;
        if (muFile != null) {
            muFileReader = openReader(muFile);
        }

        Map<String, BufferedReader> extraWigFileReaders = new HashMap<String, BufferedReader>();
        for (String extraWigFileToAverage : extraWigFilesToAverage) {
            BufferedReader extraWigFileReader = openReader(extraWigFileToAverage);
            extraWigFileReaders.put(extraWigFileToAverage, extraWigFileReader);
        }

        BufferedWriter bedFileWriter = new BufferedWriter(new FileWriter(new File(outputBedFile)));

        try {
            WigFileHelper.exportRegionsOverThresholdFromWig(name, wigFileReader, bedFileWriter, threshold, faidx, medianFilterWindow,
                    muFile, muFileReader, extraWigFilesToAverage, extraWigFileReaders);
        } finally {
            wigFileReader.close();
            bedFileWriter.close();
        }

    }

    private BufferedReader openReader(String extraWigFileToAverage) throws IOException {
        BufferedReader extraWigFileReader;
        if (extraWigFileToAverage.endsWith(".gz")) {
            extraWigFileReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(extraWigFileToAverage)))));
        } else {
            extraWigFileReader = new BufferedReader(new FileReader(new File(extraWigFileToAverage)));
        }
        return extraWigFileReader;
    }
}
