package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
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
 * Date: 1/31/13
 * Time: 10:01 AM
 */
public abstract class VariantExtractionCommand extends BaseCloudbreakCommand {
    @Parameter(names = {"--name"}, required = true)
    String name;
    @Parameter(names = {"--targetIsize"}, required = true)
    int targetIsize;
    @Parameter(names = {"--targetIsizeSD"}, required = true)
    int targetIsizeSD;
    @Parameter(names = {"--faidx"}, required = true)
    String faidxFileName;
    @Parameter(names = {"--threshold"})
    Double threshold = 1.68;
    @Parameter(names = {"--medianFilterWindow"})
    int medianFilterWindow = 5;


    protected abstract String getVariantType();

}
