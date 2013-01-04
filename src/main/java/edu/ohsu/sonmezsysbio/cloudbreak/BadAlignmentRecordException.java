package edu.ohsu.sonmezsysbio.cloudbreak;

/**
* Created by IntelliJ IDEA.
* User: cwhelan
* Date: 12/4/12
* Time: 2:08 PM
*/

/**
 * Exception thrown when unable to parse an alignment record.
 */
public class BadAlignmentRecordException extends RuntimeException {
    public BadAlignmentRecordException(Exception e) {
        super(e);
    }
}
