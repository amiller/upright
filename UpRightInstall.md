## Download ##
  * [Download binary code](http://upright.googlecode.com/files/upright.tar.gz)
  * Get source code from [svn](http://code.google.com/p/upright/source/checkout) (if you just want to run UpRight, you do not need the source code)

## Compile UpRight ##
The current version of UpRight is fully implemented in Java, so it should be able to work on  most platforms without recompilation. If you really need to do so, check out the UpRight source code from svn and type "ant" in the directory. This will generate a "bft.jar" in upright/dist/lib, which is the binary of UpRight.

## Install ##
There are no specific steps required to install UpRight. You just need to decompress the binary package (tar -xzvf upright.tar.gz). If you recompile the source code, you need to copy the dist/lib/bft.jar to the binary directory.
Notice: if you want to run upright in a cluster, then every machine must be able to access the library files. You can either put it on a shared directory, or copy it to the local directory of every machine.

## Compile your own program ##
First, see  [how to write a program with UpRight](ProgrammingWithUpRight.md) and [a simple example](UpRightExample.md).

Then, compiling the code is similar to compiling any java program, except adding the UpRight and dependency libraries:
```
javac -cp uprigh.jar:CoDec-build17-jdk13.jar:FlexiCoreProvider-1.6p3.signed.jar:netty-3.1.4.GA.jar *.java
```
For simplicity, you can also generate your own build.xml for Apache Ant. The only extra thing is adding those 3 jars to your classpath.