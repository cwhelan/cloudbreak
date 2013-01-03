package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;
import edu.ohsu.sonmezsysbio.cloudbreak.file.DFSFacade;
import edu.ohsu.sonmezsysbio.cloudbreak.file.FaidxFileHelper;
import edu.ohsu.sonmezsysbio.cloudbreak.file.WigFileHelper;
import edu.ohsu.sonmezsysbio.svpipeline.io.GenomicLocation;
import edu.ohsu.sonmezsysbio.cloudbreak.io.ReaderAndLine;
import edu.ohsu.sonmezsysbio.cloudbreak.io.SequenceReaderAndLine;
import edu.ohsu.sonmezsysbio.cloudbreak.io.TextReaderAndLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.zip.GZIPOutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/16/12
 * Time: 9:30 AM
 */
@Parameters(separators = "=", commandDescription = "Export Wig files and Bed file of deletions")
public class CommandExportWigAndBedFiles implements CloudbreakCommand {
    @Parameter(names = {"--inputHDFSDir"}, required = true)
    String inputHDFSDir;

    @Parameter(names = {"--outputPrefix"}, required = true)
    String outputPrefix;

    @Parameter(names = {"--faidx"}, required = true)
    private String faidxFileName;

    @Parameter(names = {"--text"})
    private boolean text = false;

    @Parameter(names = {"--medianFilterWindow"})
    int medianFilterWindow = 1;

    @Parameter(names = {"--averageOverSlidingWindow"})
    boolean averageOverSlidingWindow = false;

    @Parameter(names = {"--resolution"})
    int resolution = Cloudbreak.DEFAULT_RESOLUTION;

    public String getFaidxFileName() {
        return faidxFileName;
    }

    public void setFaidxFileName(String faidxFileName) {
        this.faidxFileName = faidxFileName;
    }

    public void run(Configuration conf) throws Exception {

        FaidxFileHelper faidx = new FaidxFileHelper(faidxFileName);

        String pileupFileName = outputPrefix + "_piledup_deletion_scores.wig.gz";
        String pileupBedFileName = outputPrefix + "_piledup_positive_score_regions.bed";
        String averagedFileName = outputPrefix + "_windowed_average_deletion_scores.wig";
        String averagedBedFileName = outputPrefix + "_averaged_positive_score_regions.bed";

        File outputFile = new File(pileupFileName);
        if (! outputFile.createNewFile()) {
            System.err.println("Failed to create file " + outputFile);
            return;
        }

        System.err.println("Writing file " + pileupFileName);
        Writer outputFileWriter = new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile))));
        writePiledUpDeletionScores(conf, outputFileWriter, inputHDFSDir, faidx, text);
        outputFileWriter.close();

        System.err.println("Exporting regions with positive scores into " + pileupBedFileName);
        BufferedReader pileupWigFileReader = new BufferedReader(new FileReader(new File(pileupFileName)));
        BufferedWriter piledupBedFileWriter = new BufferedWriter(new FileWriter(new File(pileupBedFileName)));
        try {
            medianFilterWindow = 1;
            WigFileHelper.exportRegionsOverThresholdFromWig(outputPrefix, pileupWigFileReader, piledupBedFileWriter, 0.0, faidx, medianFilterWindow);
        } finally {
            pileupWigFileReader.close();
            piledupBedFileWriter.close();
        }

        if (averageOverSlidingWindow) {
            System.err.println("Averaging scores over sliding window into " + averagedFileName);
            BufferedReader inFileReader = new BufferedReader(new FileReader(new File(pileupFileName)));
            BufferedWriter outFileWriter = new BufferedWriter(new FileWriter(new File(averagedFileName)));
            try {
                WigFileHelper.averageWigOverSlidingWindow(resolution, Cloudbreak.WINDOW_SIZE_IN_LINES, inFileReader, outFileWriter);
            } finally {
                inFileReader.close();
                outFileWriter.close();
            }

            System.err.println("Exporting averaged regions with positive scores into " + averagedBedFileName);
            BufferedReader averagedWigFileReader = new BufferedReader(new FileReader(new File(averagedFileName)));
            BufferedWriter averageBedFileWriter = new BufferedWriter(new FileWriter(new File(averagedBedFileName)));
            try {
                WigFileHelper.exportRegionsOverThresholdFromWig(outputPrefix, averagedWigFileReader, averageBedFileWriter, 0.0, faidx, medianFilterWindow);
            } finally {
                averagedWigFileReader.close();
                piledupBedFileWriter.close();
            }
        }

    }

    private void writePiledUpDeletionScores(Configuration conf, Writer outputFileWriter, String inputHDFSDir1, FaidxFileHelper faix, boolean text) throws IOException {
        outputFileWriter.write("track type=wiggle_0 name=\"" + outputPrefix + " Deletion Scores\"\n");

        FileSystem dfs = DistributedFileSystem.get(conf);
        FileStatus[] stati = dfs.listStatus(new Path(inputHDFSDir1));
        if (stati == null) {
            System.err.println("Could not find input directory " + inputHDFSDir1);
            return;
        }

        List<Path> inputStreams = new ArrayList<Path>();
        for (FileStatus s : stati) {
            if (s.getPath().getName().startsWith("part")) {
                Path path = s.getPath();
                System.err.println(path);
                inputStreams.add(path);
//                inputStreams.add(dfs.open(path));
            }
        }

        mergeSortedInputStreams(new DFSFacade(dfs, conf), outputFileWriter, faix, text, inputStreams);
    }

    public void mergeSortedInputStreams(DFSFacade dfsFacade, Writer outputFileWriter, FaidxFileHelper faix, boolean text, List<Path> paths) throws IOException {
        short currentChromosome = -1;
        PriorityQueue<ReaderAndLine> fileReaders = new PriorityQueue<ReaderAndLine>();
        for (Path path : paths) {
            if (text) {
                DataInputStream dataInput = new DataInputStream(dfsFacade.openPath(path));
                String line = dataInput.readLine();
                String[] fields = line.split("\t");
                fileReaders.add(new TextReaderAndLine(dataInput, new GenomicLocation(faix.getKeyForChromName(fields[0]), new Integer(fields[1])), new Double(fields[2])));
            } else {
                SequenceFile.Reader reader = new SequenceFile.Reader(dfsFacade.dfs, path, dfsFacade.conf);
                GenomicLocation gl = new GenomicLocation();
                DoubleWritable doubleWritable = new DoubleWritable();
                reader.next(gl, doubleWritable);
                //System.err.println("Read " + gl);
                fileReaders.add(new SequenceReaderAndLine(reader, gl, doubleWritable.get()));
            }
        }

        while (! fileReaders.isEmpty()) {
            ReaderAndLine minNextLine = fileReaders.poll();
            if (currentChromosome != minNextLine.getGenomicLocation().chromosome) {
                outputFileWriter.write("variableStep chrom=" + faix.getNameForChromKey(minNextLine.getGenomicLocation().chromosome) + " span=" + resolution + "\n");
                currentChromosome = minNextLine.getGenomicLocation().chromosome;
            }
            outputFileWriter.write(minNextLine.getGenomicLocation().pos + "\t" + minNextLine.getNextValue() + "\n");
            boolean gotLine;
            if (text) {
                gotLine = readNextTextLine(faix, (TextReaderAndLine) minNextLine);
            } else {
                gotLine = readNextDataLine((SequenceReaderAndLine) minNextLine);
            }

            if (gotLine) {
                fileReaders.add(minNextLine);
            } else {
                minNextLine.closeInput();
            }
        }
    }

    private boolean readNextTextLine(FaidxFileHelper faix, TextReaderAndLine minNextLine) throws IOException {
        String line = minNextLine.getDataInput().readLine();
        boolean gotLine = false;
        if (line != null) {
            String[] fields = line.split("\t");
            minNextLine.setGenomicLocation(new GenomicLocation(faix.getKeyForChromName(fields[0]), new Integer(fields[1])));
            minNextLine.setNextValue(new Double(fields[2]));
            gotLine = true;
        }
        return gotLine;
    }

    private boolean readNextDataLine(SequenceReaderAndLine minNextLine) throws IOException {
        GenomicLocation gl = new GenomicLocation();
        DoubleWritable doubleWritable = new DoubleWritable();
        boolean gotLine = minNextLine.getDataInput().next(gl, doubleWritable);
        minNextLine.setGenomicLocation(gl);
        minNextLine.setNextValue(doubleWritable.get());
        return gotLine;
    }

}
