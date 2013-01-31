package edu.ohsu.sonmezsysbio.cloudbreak.command;

import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.cloudbreak.Cloudbreak;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/22/12
 * Time: 10:33 PM
 */
@Parameters(separators = "=", commandDescription = "Extract deletion calls into a BED file")
public class CommandExtractDeletionCalls extends VariantExtractionCommand implements CloudbreakCommand {

    @Override
    protected String getVariantType() {
        return Cloudbreak.VARIANT_TYPE_DELETION;
    }

}
