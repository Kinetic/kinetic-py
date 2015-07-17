#!/bin/sh
git clone https://github.com/Seagate/kinetic-java.git
cd kinetic-java
mvn clean package -DskipTests -Dmaven.javadoc.skip=true
jar=$(echo $(pwd)/kinetic-simulator/target/*-jar-with-dependencies.jar)
mv $jar $(pwd)/../simulator.jar 