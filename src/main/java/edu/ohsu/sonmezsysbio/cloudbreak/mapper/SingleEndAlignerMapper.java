package edu.ohsu.sonmezsysbio.cloudbreak.mapper;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 7/17/12
 * Time: 5:27 PM
 */
public abstract class SingleEndAlignerMapper extends AlignerMapper {

    protected Writer getRead2FileWriter() {
        return s1FileWriter;
    }

}
