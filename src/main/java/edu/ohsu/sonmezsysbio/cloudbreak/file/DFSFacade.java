package edu.ohsu.sonmezsysbio.cloudbreak.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
* Created by IntelliJ IDEA.
* User: cwhelan
* Date: 7/9/12
* Time: 1:23 PM
*/
public class DFSFacade {
    public FileSystem dfs;
    public Configuration conf;

    public DFSFacade(FileSystem dfs, Configuration conf) {
        this.dfs = dfs;
        this.conf = conf;
    }

    public InputStream openPath(Path p) throws IOException {
        return new BufferedInputStream(dfs.open(p));
    }
}
