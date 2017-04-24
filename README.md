# NIFI-OPCUA

Nifi service and processors for accessing data via OPC UA

This project depends on the OPC Foudation UA Java implementation found here https://github.com/OPCFoundation/UA-Java

Use the directions provided to build the opc-ua-stack.XX.jar and place the jar in Nifi's lib folder



Build Instructions
------------------

Copy the opc-ua-stack.XX.jar and place it in the resources folder of opc-deploy-local project.

Update the opc-ua-stack.version variable in the pom file for the nifiopcua project

Run 'mvn clean install'