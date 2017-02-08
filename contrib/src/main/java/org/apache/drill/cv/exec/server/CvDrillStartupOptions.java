/*
 <!-- Licensed to the Apache Software Foundation (ASF) under one or more contributor 
  license agreements. See the NOTICE file distributed with this work for additional 
  information regarding copyright ownership. The ASF licenses this file to 
  You under the Apache License, Version 2.0 (the "License"); you may not use 
  this file except in compliance with the License. You may obtain a copy of 
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required 
  by applicable law or agreed to in writing, software distributed under the 
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
  OF ANY KIND, either express or implied. See the License for the specific 
  language governing permissions and limitations under the License. -->
 */
package org.apache.drill.cv.exec.server;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;


@Parameters(separators = "=")
public class CvDrillStartupOptions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(CvDrillStartupOptions.class);

  @Parameter(names = { "-h", "--help" }, description = "Provide description of usage.", help = true)
  private boolean help = false;

  @Parameter(names = { "-d", "--debug" }, description = "Whether you want to run the program in debug mode.", required = false)
  private boolean debug = false;

  @Parameter(names = { "-c", "--config" }, description = "Configuration file you want to load.  Defaults to loading 'drill-override.conf' from the classpath.", required = false)
  private String configLocation = null;

  @Parameter(names = { "-stop", "--shutdown" }, description = "Intiates Shutdown request to a running drillbit.", required = false)
  private boolean shutdown = false;

  @Parameter(names = { "-p", "--port" }, description = "drillbit http port.", required = false)
  private String port = null;
  
  @Parameter(names = { "-sk", "--stopkey" }, description = "drillbit jetty server stop key.", required = false)
  private String stopKey = null;
  
  @Parameter
  private List<String> exccess = new ArrayList<String>();

  public boolean isDebug() {
    return debug;
  }

  public String getConfigLocation() {
    return configLocation;
  }

  public List<String> getExccess() {
    return exccess;
  }

  public boolean isShutdown() {
    return shutdown;
  }

  public String getPort() {
    return port;
  }

  public String getStopKey() {
    return stopKey;
  }

  public static CvDrillStartupOptions parse(String[] cliArgs) {
    logger.debug("Parsing arguments.");
    CvDrillStartupOptions args = new CvDrillStartupOptions();
    JCommander jc = new JCommander(args, cliArgs);
    if (args.help) {
      jc.usage();
      System.exit(0);
    }
    return args;
  }
}
