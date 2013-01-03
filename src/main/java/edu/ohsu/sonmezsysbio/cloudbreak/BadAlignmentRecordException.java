package edu.ohsu.sonmezsysbio.cloudbreak;

/**
* Created by IntelliJ IDEA.
* User: cwhelan
* Date: 12/4/12
* Time: 2:08 PM
*/
public class BadAlignmentRecordException extends RuntimeException {
    public BadAlignmentRecordException(Exception e) {
        super(e);
    }
}
