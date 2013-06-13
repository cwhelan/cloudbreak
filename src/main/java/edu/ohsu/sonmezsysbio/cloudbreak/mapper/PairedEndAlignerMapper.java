package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.Writer;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 6/12/13
 * Time: 4:18 PM
 */
public class PairedEndAlignerMapper extends AlignerMapper {

    protected Writer getRead2FileWriter() {
        return s2FileWriter;
    }
}
