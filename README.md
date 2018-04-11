// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

# Hadoop Adapter for Lustre (HAL)

HAL is used to make Hadoop work on Lustre, instead of HDFS, without any Lustre changes. This project is developed by the Intel High Performance Data Division, one of the main contributors to the Lustre Filesystem. We welcome community involvement and contributions.

## Build and Install instructions

Requirements:

* JDK 1.7+
* Maven 3.0 or later
* Internet connection for first build (to fetch all Maven and Hadoop dependencies)

Maven build goals:

 * Clean				: mvn clean
 * Compile				: mvn compile
 * Run tests				: mvn test
 * Generate Eclipse files		: mvn eclipse:eclipse
 * Create JAR/Install JAR in M2 cache   : mvn install

Installation:

Please refer "Installation of HAL on Apache Hadoop.md" in this directory for details.
