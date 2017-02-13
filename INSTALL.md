# How to build and run cvdrill (Apache Drill with SOLR Storage plugin)

## Prerequisites

Currently, the Apache Drill build process is known to work on Linux, Windows and OSX.  To build, you need to have the following software installed on your system to successfully complete a build. 
  * Java 7
  * Maven 3.x

## Confirm settings
    # java -version
    java version "1.7.0_09"
    Java(TM) SE Runtime Environment (build 1.7.0_09-b05)
    Java HotSpot(TM) 64-Bit Server VM (build 23.5-b02, mixed mode)
    
    # mvn --version
    Apache Maven 3.0.3 (r1075438; 2011-02-28 09:31:09-0800)

## Checkout

    git clone https://github.com/CommvaultEngg/cvdrill.git
    
## Build

    cd cvdrill
    mvn clean install

## Explode tarball in installation directory##
	
	mkdir "C:\Program Files\CvDrill"
	tar xvzf build\*.tar.gz --strip=1 -C "C:\Program Files\CvDrill"

## Start SQLLINE (which starts Drill in embedded mode[1]) ##
	
	cd "C:\Program Files\CvDrill\conf"
	sqlline.bat -u "jdbc:drill:zk=local"

[1] Please refer to [Apache Drill Installation](https://drill.apache.org/docs/install-drill/).

## Run a query (on SOLR storage plugin) ##
SOLR storage plugin can comes as one of the default plugin with cvdrill. Once sqlline is started drill Web UI can be accessed in browser through http://localhost:8047/. SOLR url can be updated from the Storage tab , there will be new plugin called solr appearing along with the drill's default plugins[2]. Once the Storage plugin is updated Query can be run from either sqlline console or from drill Web UI.
 
	SELECT id,name,manu,manu_id_s,inStock FROM solr.`techproducts`;

NOTE: The SOLR url should be accessible from the system where cvdrill is running.
[2] Please refer to [Apache Drill Plugin Configuration](https://drill.apache.org/docs/plugin-configuration-basics/). 

## More information ##

More information on running a query or connecting to BI tools can be found in [Apache Drill Documentation](http://drill.apache.org/docs/).