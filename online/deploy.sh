#!/bin/bash

ssh root@10.105.75.174 "/opt/tomcat/bin/shutdown.sh; rm -rf /opt/tomcat/webapps/pagemining*;"
scp target/online-1.0-SNAPSHOT.war root@10.105.75.174:/opt/tomcat/webapps/pagemining.war
ssh root@10.105.75.174 "/opt/tomcat/bin/startup.sh"
