#!/bin/sh
git clone https://github.com/Seagate/kinetic-java.git
cd kinetic-java
mvn clean package -DskipTests -Dmaven.javadoc.skip=true