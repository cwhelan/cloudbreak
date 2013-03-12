package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.file.WigFileHelper;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/22/12
 * Time: 10:33 PM
 */
@Parameters(separators = "=", commandDescription = "Extract insertion calls into a BED file")
public class CommandExtractInsertionCalls extends VariantExtractionCommand implements CloudbreakCommand {

    @Parameter(names = {"--inputWigFile"}, required = true)
    String inputWigFile;
    @Parameter(names = {"--outputBedFile"}, required = true)
    String outputBedFile;
    @Parameter(names = {"--muFile"}, required = true)
    String muFile = null;
    @Parameter(names = {"--w0File"}, required = true)
    String w0File = null;

    public void run(Configuration conf) throws Exception {
        FaidxFileHelper faidx = new FaidxFileHelper(faidxFileName);

        BufferedReader wigFileReader = openReader(inputWigFile);

        BufferedReader muFileReader;
        muFileReader = openReader(muFile);

        Map<String, BufferedReader> extraWigFileReaders = new HashMap<String, BufferedReader>();
        BufferedReader w0FileReader;
        w0FileReader = openReader(w0File);
        extraWigFileReaders.put(w0File, w0FileReader);

        BufferedWriter bedFileWriter = new BufferedWriter(new FileWriter(new File(outputBedFile)));

        try {
            WigFileHelper.exportRegionsOverThresholdFromWig(name, wigFileReader, bedFileWriter, threshold, faidx, medianFilterWindow,
                    muFile, muFileReader, Arrays.asList(w0File), extraWigFileReaders, targetIsize, targetIsizeSD, getVariantType());
        } finally {
            wigFileReader.close();
            bedFileWriter.close();
        }

    }

    @Override
    protected String getVariantType() {
        return Cloudbreak.VARIANT_TYPE_INSERTION;
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
