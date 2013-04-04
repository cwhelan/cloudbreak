package edu.ohsu.sonmezsysbio.cloudbreak.cloud;

import com.google.common.base.Predicates;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.domain.Statements;

import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 3/30/13
 * Time: 11:34 AM
 */
public class CloudCluster {

    private final ClusterSpec clusterSpec;
    private Cluster cluster;

    public CloudCluster() throws ConfigurationException {
        clusterSpec = new ClusterSpec(new PropertiesConfiguration("whirr/cloudbreak-whirr.properties"), false);
    }

    public void launch() throws IOException, InterruptedException {
        ClusterControllerFactory factory = new ClusterControllerFactory();
        ClusterController controller = factory.create(clusterSpec.getServiceName());
        System.err.println("Starting cluster " + clusterSpec.getClusterName());
        cluster = controller.launchCluster(clusterSpec);
    }

    public void destroy() throws IOException, InterruptedException {
        ClusterControllerFactory factory = new ClusterControllerFactory();
        ClusterController controller = factory.create(clusterSpec.getServiceName());
        System.err.println("Destroying cluster " + clusterSpec.getClusterName());
        controller.destroyCluster(clusterSpec);
    }

    public void runScript(String scriptName) throws IOException, RunScriptOnNodesException, InterruptedException {
        ClusterControllerFactory factory = new ClusterControllerFactory();
        ClusterController controller = factory.create(clusterSpec.getServiceName());

        Cluster.Instance instance = controller.getInstances(clusterSpec).iterator().next();
        System.err.println("running scripts on instance " + instance);

        StatementBuilder statementBuilder = new StatementBuilder();
        statementBuilder.name(instance.getPublicHostName());

        ArrayList<String> lines = readScriptLines(scriptName);

        statementBuilder.addStatements(Statements.appendFile("prepCluster", lines));
        Map<? extends NodeMetadata, ExecResponse> responses =
                controller.runScriptOnNodesMatching(clusterSpec,
                        Predicates.equalTo(instance.getNodeMetadata()),
                        statementBuilder.build(clusterSpec, instance));
        for (ExecResponse execResponse : responses.values()) {
            System.err.println(execResponse);
        }
    }

    private ArrayList<String> readScriptLines(String foo) throws FileNotFoundException {
        String fileName = foo;
        Scanner s = new Scanner(new File(fileName)).useDelimiter("\n");
        ArrayList<String> lines = new ArrayList<String>();
        while (s.hasNext()){
            lines.add(s.next());
        }
        s.close();
        return lines;
    }
}
