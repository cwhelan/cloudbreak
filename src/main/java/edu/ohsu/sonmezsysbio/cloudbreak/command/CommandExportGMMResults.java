package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.file.DFSFacade;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.io.GMMResultsReaderAndLine;
import edu.ohsu.sonmezsysbio.cloudbreak.io.TextReaderAndLine;
import edu.ohsu.sonmezsysbio.cloudbreak.reducer.GMMScorerResults;
import edu.ohsu.sonmezsysbio.svpipeline.io.GenomicLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/16/12
 * Time: 9:30 AM
 */
@Parameters(separators = "=", commandDescription = "Export Wig files and Bed file of deletions")
public class CommandExportGMMResults implements CloudbreakCommand {

    private static org.apache.log4j.Logger logger = Logger.getLogger(CommandExportGMMResults.class);

    @Parameter(names = {"--inputHDFSDir"}, required = true)
    String inputHDFSDir;

    @Parameter(names = {"--outputPrefix"}, required = true)
    String outputPrefix;

    @Parameter(names = {"--faidx"}, required = true)
    private String faidxFileName;

    @Parameter(names = {"--resolution"})
    int resolution = Cloudbreak.DEFAULT_RESOLUTION;

    public void run(Configuration conf) throws Exception {

        FaidxFileHelper faidx = new FaidxFileHelper(faidxFileName);

        List<String> outputs = Arrays.asList("w0", "mu2", "nodelOneComponentLikelihood", "twoComponentLikelihood",
                "lrHeterozygous", "cleanCoverage", "c1membership",
                "c2membership", "weightedC1membership", "weightedC2membership");

        Map<String, Writer> writers = createWritersForOutputs(outputs);

        try {
            writeGMMResultWigFiles(conf, writers,
                    inputHDFSDir, faidx);
        } finally {
            for (Writer writer : writers.values()) {
                writer.close();
            }
        }


    }

    private Map<String, Writer> createWritersForOutputs(List<String> outputs) throws IOException {
        Map<String, Writer> writers = new HashMap<String, Writer>();
        for (String outputName : outputs) {
            String filename = outputPrefix + "_" + outputName + ".wig.gz";
            Writer writer = createWriter(filename);
            if (writer == null) throw new RuntimeException("Failed to create file");
            writers.put(outputName, writer);
        }
        return writers;
    }

    private Writer createWriter(String fileName) throws IOException {
        File outputFile = new File(fileName);
        if (! outputFile.createNewFile()) {
            logger.error("Failed to create file " + outputFile);
            return null;
        }

        logger.info("Writing file " + fileName);
        return new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile))));
    }

    private void writeGMMResultWigFiles(Configuration conf, Map<String, Writer> writers, String inputHDFSDir1,
                                        FaidxFileHelper faix
    ) throws IOException, NoSuchFieldException, IllegalAccessException {
        for (String name : writers.keySet()) {
            Writer writer = writers.get(name);
            writer.write("track type=wiggle_0 name=\"" + outputPrefix + " " + name + "\"\n");
        }

        FileSystem dfs = DistributedFileSystem.get(conf);
        FileStatus[] stati = dfs.listStatus(new Path(inputHDFSDir1));
        if (stati == null) {
            logger.error("Could not find input directory " + inputHDFSDir1);
            return;
        }

        List<Path> inputStreams = new ArrayList<Path>();
        for (FileStatus s : stati) {
            if (s.getPath().getName().startsWith("part")) {
                Path path = s.getPath();
                logger.info(path);
                inputStreams.add(path);
//                inputStreams.add(dfs.open(path));
            }
        }

        mergeSortedInputStreams(new DFSFacade(dfs, conf), writers,
                faix, inputStreams);
    }

    public void mergeSortedInputStreams(DFSFacade dfsFacade, Map<String, Writer> writers, FaidxFileHelper faix,
                                        List<Path> paths) throws IOException, NoSuchFieldException, IllegalAccessException {
        short currentChromosome = -1;
        PriorityQueue<GMMResultsReaderAndLine> fileReaders = new PriorityQueue<GMMResultsReaderAndLine>();
        for (Path path : paths) {
            SequenceFile.Reader reader = new SequenceFile.Reader(dfsFacade.dfs, path, dfsFacade.conf);
            GenomicLocation gl = new GenomicLocation();
            GMMScorerResults results = new GMMScorerResults();
            reader.next(gl, results);
            fileReaders.add(new GMMResultsReaderAndLine(reader, gl, results));
        }

        while (! fileReaders.isEmpty()) {
            GMMResultsReaderAndLine minNextLine = fileReaders.poll();
            if (currentChromosome != minNextLine.getGenomicLocation().chromosome) {
                for (Writer writer : writers.values()) {
                    writeChromHeader(writer, faix, minNextLine);
                }

                currentChromosome = minNextLine.getGenomicLocation().chromosome;
            }

            for (String name : writers.keySet()) {
                Field f = GMMScorerResults.class.getField(name);
                Writer writer = writers.get(name);
                writer.write(minNextLine.getGenomicLocation().pos + "\t" + f.getDouble(minNextLine.getNextValue()) + "\n");
            }

            boolean gotLine;
            gotLine = readNextDataLine(minNextLine);

            if (gotLine) {
                fileReaders.add(minNextLine);
            } else {
                minNextLine.closeInput();
            }
        }
    }

    private void writeChromHeader(Writer w0outputFileWriter, FaidxFileHelper faix, GMMResultsReaderAndLine minNextLine) throws IOException {
        w0outputFileWriter.write("variableStep chrom=" + faix.getNameForChromKey(minNextLine.getGenomicLocation().chromosome) + " span=" + resolution + "\n");
    }

    private boolean readNextDataLine(GMMResultsReaderAndLine minNextLine) throws IOException {
        GenomicLocation gl = new GenomicLocation();
        GMMScorerResults results = new GMMScorerResults();
        boolean gotLine = minNextLine.getReader().next(gl, results);
        minNextLine.setGenomicLocation(gl);
        minNextLine.setNextValue(results);
        return gotLine;
    }

}
