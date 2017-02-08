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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.StackTrace;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.cv.exec.coord.zk.CvZKClusterCoordinator;
import org.apache.drill.cv.exec.server.rest.CvDrillWebServer;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ClusterCoordinator.RegistrationHandle;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.service.ServiceEngine;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.store.sys.PersistentStoreRegistry;
import org.apache.drill.exec.store.sys.store.provider.CachingPersistentStoreProvider;
import org.apache.drill.exec.store.sys.store.provider.LocalPersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.apache.zookeeper.Environment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

/**
 * Starts, tracks and stops all the required services for a CvDrillBit daemon to
 * work.
 */
public class CvDrillBit implements AutoCloseable {
  /**
   * Shutdown hook for Drillbit. Closes the drillbit, and reports on errors that
   * occur during closure, as well as the location the drillbit was started
   * from.
   */
  private static class ShutdownThread extends Thread {
    private final static AtomicInteger idCounter = new AtomicInteger(0);
    private final CvDrillBit cvDrillbit;
    private final StackTrace stackTrace;

    /**
     * Constructor.
     *
     * @param drillbit
     *          the drillbit to close down
     * @param stackTrace
     *          the stack trace from where the Drillbit was started; use new
     *          StackTrace() to generate this
     */
    public ShutdownThread(final CvDrillBit cvDrillbit,
        final StackTrace stackTrace) {
      this.cvDrillbit = cvDrillbit;
      this.stackTrace = stackTrace;
      /*
       * TODO should we try to determine a test class name? See
       * https://blogs.oracle.com/tor/entry/how_to_determine_the_junit
       */

      setName("CvDrillBit-ShutdownHook#" + ShutdownThread.idCounter.getAndIncrement());
    }

    @Override
    public void run() {
      CvDrillBit.logger.info("Received shutdown request.");
      try {
        /*
         * We can avoid metrics deregistration concurrency issues by only
         * closing one drillbit at a time. To enforce that, we synchronize on a
         * convenient singleton object.
         */
        synchronized (ShutdownThread.idCounter) {
          cvDrillbit.close();
        }
      } catch (final Exception e) {
        throw new RuntimeException(
            "Caught exception closing Drillbit started from\n" + stackTrace, e);
      }
    }
  }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(Drillbit.class);

  static {
    Environment.logEnv("CvDrillbit environment: ", CvDrillBit.logger);
  }

  private final static String SYSTEM_OPTIONS_NAME = "org.apache.drill.exec.server.Drillbit.system_options";

  public static void main(String[] cli) throws DrillbitStartupException {
    final CvDrillStartupOptions options = CvDrillStartupOptions.parse(cli);
    if (options.isShutdown()) {
      int shutDownStatus = -1;
      String urlStr = "http://localhost:" + options.getPort()
      + "/shutdown?token=" + options.getStopKey();
      CvDrillBit.logger.info(" CvDrillbit shutdown request recieved to url [ " + urlStr
          + " ]");
      URL url;
      try {
        url = new URL(urlStr);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.connect();
        CvDrillBit.logger.info("Drillbit running at port : " + options.getPort()
        + " was brought down. " + "Response Message "
        + connection.getResponseCode());
        shutDownStatus = 0;
      } catch (IOException e) {
        CvDrillBit.logger.error(" failed to stop cvdrillbit..." + e.getMessage());
      }
      System.exit(shutDownStatus);
    }
    CvDrillBit.start(options);
  }

  public static CvDrillBit start(final CvDrillStartupOptions options)
      throws DrillbitStartupException {
    return CvDrillBit.start(DrillConfig.create(options.getConfigLocation()), null,options);
  }

  public static CvDrillBit start(final DrillConfig config)
      throws DrillbitStartupException {
    return CvDrillBit.start(config, null,null);
  }

  public static CvDrillBit start(final DrillConfig config,
      final RemoteServiceSet remoteServiceSet,final CvDrillStartupOptions options) throws DrillbitStartupException {
    CvDrillBit.logger.debug("Starting new CvDrillbit.");
    // TODO: allow passing as a parameter
    ScanResult classpathScan = ClassPathScanner.fromPrescan(config);
    CvDrillBit bit;
    try {

      bit = new CvDrillBit(config, remoteServiceSet, classpathScan);
    } catch (final Exception ex) {
      throw new DrillbitStartupException(
          "Failure while initializing values in CvDrillbit.", ex);
    }

    try {
      String webserverPort = null;
      String embeddedJettyStopKey = null;
      if (options != null){
        webserverPort = options.getPort();
        embeddedJettyStopKey = options.getStopKey();
      }
      bit.run(webserverPort,embeddedJettyStopKey);
    } catch (final Exception e) {
      bit.close();
      throw new DrillbitStartupException(
          "Failure during initial startup of CvDrillbit.", e);
    }
    CvDrillBit.logger.debug("Started new CvDrillbit.");
    return bit;
  }

  private static String stripQuotes(final String s, final String systemProp) {
    if (s.isEmpty()) {
      return s;
    }

    final char cFirst = s.charAt(0);
    final char cLast = s.charAt(s.length() - 1);
    if ((cFirst == '"') || (cFirst == '\'')) {
      if (cLast != cFirst) {
        CvDrillBit.throwInvalidSystemOption(systemProp,
            "quoted value does not have closing quote");
      }

      return s.substring(1, s.length() - 2); // strip the quotes
    }

    if ((cLast == '"') || (cLast == '\'')) {
      CvDrillBit.throwInvalidSystemOption(systemProp, "value has unbalanced closing quote");
    }

    // return as-is
    return s;
  }

  private static void throwInvalidSystemOption(final String systemProp,
      final String errorMessage) {
    throw new IllegalStateException("Property \"" + CvDrillBit.SYSTEM_OPTIONS_NAME
        + "\" part \"" + systemProp + "\" " + errorMessage + ".");
  }

  private boolean isClosed = false;

  private final ClusterCoordinator coord;
  private final ServiceEngine engine;
  private final PersistentStoreProvider storeProvider;
  private final WorkManager manager;
  private final BootStrapContext context;
  private final CvDrillWebServer webServer;
  private RegistrationHandle registrationHandle;
  private volatile StoragePluginRegistry storageRegistry;

  @VisibleForTesting
  public CvDrillBit(final DrillConfig config, final RemoteServiceSet serviceSet)
      throws Exception {
    this(config, serviceSet, ClassPathScanner.fromPrescan(config));
  }

  public CvDrillBit(final DrillConfig config,
      final RemoteServiceSet serviceSet, final ScanResult classpathScan)
          throws Exception {
    final Stopwatch w = Stopwatch.createStarted();
    CvDrillBit.logger.debug("Construction started.");
    final boolean allowPortHunting = serviceSet != null;
    context = new BootStrapContext(config, classpathScan);
    manager = new WorkManager(context);


    webServer = new CvDrillWebServer(config, context.getMetrics(), manager);
    boolean isDistributedMode = false;

    if (serviceSet != null) {
      coord = serviceSet.getCoordinator();
      storeProvider = new CachingPersistentStoreProvider(new LocalPersistentStoreProvider(config));
    } else {
      coord = new CvZKClusterCoordinator(config);
      storeProvider = new PersistentStoreRegistry(this.coord, config).newPStoreProvider();
      isDistributedMode = true;
    }
    engine = new ServiceEngine(manager.getControlMessageHandler(), manager.getUserWorker(), context,
        manager.getWorkBus(), manager.getBee(), allowPortHunting, isDistributedMode);

    CvDrillBit.logger.info("Construction completed ({} ms).",
        w.elapsed(TimeUnit.MILLISECONDS));
  }

  @Override
  public synchronized void close() {
    // avoid complaints about double closing
    if (isClosed) {
      return;
    }
    final Stopwatch w = Stopwatch.createStarted();
    CvDrillBit.logger.debug("Shutdown begun.");

    // wait for anything that is running to complete
    manager.waitToExit();

    if (coord != null && registrationHandle != null) {
      coord.unregister(registrationHandle);
    }
    try {
      Thread.sleep(context.getConfig().getInt(ExecConstants.ZK_REFRESH) * 2);
    } catch (final InterruptedException e) {
      CvDrillBit.logger
      .warn("Interrupted while sleeping during coordination deregistration.");

      // Preserve evidence that the interruption occurred so that code higher up
      // on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }

    try {
      AutoCloseables.close(webServer, engine, storeProvider, coord, manager,
          context);
    } catch (Exception e) {
      CvDrillBit.logger.warn("Failure on close()", e);
    }

    CvDrillBit.logger
    .info("Shutdown completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
    isClosed = true;
  }

  public DrillbitContext getContext() {
    return manager.getContext();
  }

  private void javaPropertiesToSystemOptions() {
    // get the system options property
    final String allSystemProps = System.getProperty(CvDrillBit.SYSTEM_OPTIONS_NAME);
    if ((allSystemProps == null) || allSystemProps.isEmpty()) {
      return;
    }

    final OptionManager optionManager = getContext().getOptionManager();

    // parse out the properties, validate, and then set them
    final String systemProps[] = allSystemProps.split(",");
    for (final String systemProp : systemProps) {
      final String keyValue[] = systemProp.split("=");
      if (keyValue.length != 2) {
        CvDrillBit.throwInvalidSystemOption(systemProp,
            "does not contain a key=value assignment");
      }

      final String optionName = keyValue[0].trim();
      if (optionName.isEmpty()) {
        CvDrillBit.throwInvalidSystemOption(systemProp,
            "does not contain a key before the assignment");
      }

      final String optionString = CvDrillBit.stripQuotes(keyValue[1].trim(), systemProp);
      if (optionString.isEmpty()) {
        CvDrillBit.throwInvalidSystemOption(systemProp,
            "does not contain a value after the assignment");
      }

      final OptionValue defaultValue = optionManager.getOption(optionName);
      if (defaultValue == null) {
        CvDrillBit.throwInvalidSystemOption(systemProp,
            "does not specify a valid option name");
      }
      if (defaultValue.type != OptionType.SYSTEM) {
        CvDrillBit.throwInvalidSystemOption(systemProp,
            "does not specify a SYSTEM option ");
      }

      final OptionValue optionValue = OptionValue.createOption(
          defaultValue.kind, OptionType.SYSTEM, optionName, optionString);
      optionManager.setOption(optionValue);
    }
  }

  public void run(String webserverPort, String embeddedJettyStopKey) throws Exception {
    final Stopwatch w = Stopwatch.createStarted();
    CvDrillBit.logger.debug("Startup begun.");
    coord.start(10000);
    storeProvider.start();
    final DrillbitEndpoint md = engine.start();
    manager.start(md, engine.getController(),
        engine.getDataConnectionCreator(), coord, storeProvider);
    final DrillbitContext drillbitContext = manager.getContext();
    drillbitContext.getStorage().init();
    drillbitContext.getOptionManager().init();
    javaPropertiesToSystemOptions();
    registrationHandle = coord.register(md);

    webServer.setEmbeddedJettyStopKey(embeddedJettyStopKey);
    webServer.setWebserverPort(webserverPort);
    webServer.start();

    Runtime.getRuntime().addShutdownHook(
        new ShutdownThread(this, new StackTrace()));
    CvDrillBit.logger.info("Startup completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
  }
}
