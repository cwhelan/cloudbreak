package edu.ohsu.sonmezsysbio.cloudbreak.io;

import edu.ohsu.sonmezsysbio.cloudbreak.AlignmentRecord;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 12/9/12
 * Time: 9:21 PM
 */
public interface AlignmentRecordFilter {

    public boolean passes(AlignmentRecord record);
}
