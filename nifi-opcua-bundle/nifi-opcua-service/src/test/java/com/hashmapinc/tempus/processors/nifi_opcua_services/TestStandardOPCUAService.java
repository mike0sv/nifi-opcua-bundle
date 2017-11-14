/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hashmapinc.tempus.processors.nifi_opcua_services;

import com.hashmap.tempus.opc.test.server.TestServer;
import com.hashmapinc.tempus.processors.OPCUAService;
import com.hashmapinc.tempus.processors.StandardOPCUAService;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opcfoundation.ua.builtintypes.ExpandedNodeId;
import org.opcfoundation.ua.builtintypes.NodeId;
import org.opcfoundation.ua.builtintypes.UnsignedInteger;
import org.opcfoundation.ua.core.Identifiers;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestStandardOPCUAService {

    TestServer server;

    @Before
    public void init() {
        try {
            server = new TestServer(45678);
        } catch (Exception e) {
            e.printStackTrace();
        }
        server.start();
    }

    @Test
    public void testMissingPropertyValues() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardOPCUAService service = new StandardOPCUAService();
        final Map<String, String> properties = new HashMap<String, String>();
        runner.addControllerService("test-bad1", service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testGetNodeList() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardOPCUAService service = new StandardOPCUAService();

        StringBuilder stringBuilder = new StringBuilder();

        runner.addControllerService("hi", service);

        runner.setProperty(service, StandardOPCUAService.APPLICATION_NAME, "nifi");
        runner.setProperty(service, StandardOPCUAService.ENDPOINT, "opc.tcp://127.0.0.1:45678/test");
        runner.setProperty(service, StandardOPCUAService.SECURITY_POLICY, "NONE");

        runner.enableControllerService(service);
        List<ExpandedNodeId> ids = new ArrayList<>();
        ids.add(new ExpandedNodeId((Identifiers.RootFolder)));
        stringBuilder.append(service.getNameSpace("No", 3, ids, new UnsignedInteger(1000)));

        String result = stringBuilder.toString();

        runner.assertValid();

        assertNotNull(result);
        assertNotEquals(result.length(), 0);
    }

    @Test
    public void testGetNodeListFromMultipleChannels() throws InitializationException {

        //This testcase is incomplete

        final TestRunner firstRunner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardOPCUAService firstService = new StandardOPCUAService();

        firstRunner.addControllerService("firstService", firstService);

        firstRunner.setProperty(firstService, StandardOPCUAService.APPLICATION_NAME, "nifi");
        firstRunner.setProperty(firstService, StandardOPCUAService.ENDPOINT, "opc.tcp://127.0.0.1:45678/test");
        firstRunner.setProperty(firstService, StandardOPCUAService.SECURITY_POLICY, "NONE");

        firstRunner.enableControllerService(firstService);

        String starting_node = "";
        List<ExpandedNodeId> ids = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();

        String[] splits = NodeId.parseNodeId(starting_node).toString().split(",");

        for (String split : splits) {
            ids.add(new ExpandedNodeId(NodeId.parseNodeId(split)));
        }

        stringBuilder.append(firstService.getNameSpace("No", 0, ids, new UnsignedInteger(1000)));

        String firstResult = stringBuilder.toString();

        firstRunner.assertValid();

        assertNotNull(firstResult);
        assertNotEquals(firstResult.length(), 0);

    }

    @After
    public void shutdown(){
        server.stop();
    }

/*    @Test
    public void testService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMyService service = new StandardMyService();
        runner.addControllerService("test-good", service);

        runner.setProperty(service, StandardMyService.MY_PROPERTY, "test-value");
        runner.enableControllerService(service);

        runner.assertValid(service);
    }*/

}
