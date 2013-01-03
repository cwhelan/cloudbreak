package edu.ohsu.sonmezsysbio.cloudbreak.file;

import edu.ohsu.sonmezsysbio.cloudbreak.ReadGroupInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/9/12
 * Time: 10:03 AM
 */
public class ReadGroupInfoFileHelper {

    public Map<String,Short> readReadGroupIdsByHDFSPath(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        try {
            return readReadGroupIdsByHDFSPath(reader);
        } finally {
            reader.close();
        }
    }

    public Map<String,Short> readReadGroupIdsByHDFSPath(BufferedReader reader) throws IOException {
         String line;
         short rgIdx = 0;
         Map<String,Short> rgsByPath = new HashMap<String,Short>();
         while ((line = reader.readLine()) != null) {
             String rgPath = line.split("\t")[5];
             rgsByPath.put(rgPath, rgIdx);
             rgIdx++;
         }
         return rgsByPath;
    }

    public Map<Short, ReadGroupInfo> readReadGroupsById(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        try {
            return readReadGroupsById(reader);
        }finally {
            reader.close();
        }
    }

    public Map<Short, ReadGroupInfo> readReadGroupsById(BufferedReader reader) throws IOException {
        String line;
        Map<Short,ReadGroupInfo> rgsById = new HashMap<Short,ReadGroupInfo>();
        short rgIdx = 0;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("\\t");
            ReadGroupInfo readGroupInfo = new ReadGroupInfo();
            readGroupInfo.readGroupName = fields[0];
            readGroupInfo.libraryName = fields[1];
            readGroupInfo.isize = Integer.parseInt(fields[2]);
            readGroupInfo.isizeSD = Integer.parseInt(fields[3]);
            readGroupInfo.matePair = Boolean.parseBoolean(fields[4]);
            readGroupInfo.hdfsPath = fields[5];
            rgsById.put(rgIdx, readGroupInfo);
            rgIdx++;
        }
        return rgsById;
    }
}
