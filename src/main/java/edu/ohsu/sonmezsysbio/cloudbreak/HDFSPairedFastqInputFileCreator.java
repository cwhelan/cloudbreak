package edu.ohsu.sonmezsysbio.cloudbreak;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/18/11
 * Time: 2:48 PM
 */
public class HDFSPairedFastqInputFileCreator {
    private String hdfsDir;
    private String file1;
    private String file2;

    public HDFSPairedFastqInputFileCreator(String hdfsDir, String file1, String file2) {
        this.hdfsDir = hdfsDir;
        this.file1 = file1;
        this.file2 = file2;
    }

    public void uploadInputFilesToHdfs() throws IOException {
        BufferedReader f1Reader = new BufferedReader(new FileReader(file1));
        BufferedReader f2Reader = new BufferedReader(new FileReader(file2));

        String f1IdLine = f1Reader.readLine();
        while (f1IdLine != null) {
            String f1Id = f1IdLine.substring(1);
            String f1Seq = f1Reader.readLine();
            f1Reader.readLine();
            String f1Quals = f1Reader.readLine();

            String f2IdLine = f2Reader.readLine();
            String f2Id = f2IdLine.substring(1);
            String f2Seq = f2Reader.readLine();
            f1Reader.readLine();
            String f2Quals = f2Reader.readLine();

            String readPairName = f1Id.substring(0, f1Id.length() - 2);


        }
    }
}
