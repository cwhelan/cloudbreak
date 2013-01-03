package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/18/11
 * Time: 2:05 PM
 */
@Parameters(separators = "=", commandDescription = "SV detection Hadoop pipeline")
public class CommanderMain {
    @Parameter(names = {"--help", "-help", "-?", "-h"}, description = "Help")
    private boolean help;

    public boolean isHelp() {
        return help;
    }
}
