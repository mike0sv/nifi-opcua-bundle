<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/Tempus_Logo_Black_with_TagLine.png" width="950" height="245" alt="Hashmap, Inc Tempus"/>

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

# nifi-opcua-bundle
These processors and associated controller service allow NiFi access to OPC UA servers in a read-only fashion. This bundle
provides 2 processors, GetOPCNodeList and GetOPCData. GetNodeIds allows access to the tags that are currently in the OPCUA server,
GetOPCData takes a list of tags and queries the OPC UA server for the values. The StandardOPCUAService provides the connectivity
to the OPCUA server so that multiple processors can leverage the same connection/session information.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [License](#license)

## Features

This processor aims to provide a few key features:

* Access to list the tag information currently in the OPC UA server
* Access to query the data currently in the OPC UA server
* Optional null value exclusion
* Configurable timestamp selection

## Requirements

* JDK 1.8 at a minimum
* Maven 3.1 or newer
* Git client (to build locally)
* OPC Foundation Stack (instructions to build below)

## Getting Started

### Build the OPC Foundation Stack

Clone the OPC Foundation GitHub repository

    https://github.com/OPCFoundation/UA-Java.git

Change directory into the UA-Java directory

    cd UA-Java
    
Execute the package phase (NOTE: at the time of this writing, there were test failures due to invalid tests, there are currently PR's
out there to address these, but they have not been merged into master, therefore we need to skip tests)

    mvn package -DskipTests
    
### Setup the local build environment for the processor    

To build the library and get started first off clone the GitHub repository 

    git clone https://github.com/hashmapinc/nifi-opcua-bundle.git
    
Copy the jar from the previous step where we built the OPC Foundation code from the cloned repo of the OPC foundation 
code (Where repo_location is the location of where the cloned repo is and {version} is the version of the OPC Foundation 
code that was cloned.)

    {repo_location}/UA-Java/target/opc-ua-stack-{version}-SNAPSHOT.jar

Place that file into the following directory (where repo_location is the location of where the nifi-opcua-bundle repo was cloned.)

    {repo_location}/nifi-opcua-bundle/opc-deploy-local/src/main/resources
    
Change directory into the root of the nifi-opcua-bundle codebase located in

    {repo_location}/nifi-opcua-bundle/nifi-opc-ua
    
Execute a maven clean install

    mvn clean install
    
A Build success message should appear
   
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 9.384 s
    [INFO] Finished at: 2017-08-01T08:27:00-05:00
    [INFO] Final Memory: 31M/423M
    [INFO] ------------------------------------------------------------------------

A NAR file should be located in the following directory

    {repo_location}/nifi-opcua-bundle/nifi-opc-ua/nifi-opcua-bundle/nifi-opcua-bundle-nar/target
    
Copy this NAR file to the /lib directory and restart (or start) Nifi.

## Usage
Once NiFi is restarted the processors should be able to be added as normal, by dragging a processor onto the NiFi canvas.



## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 

