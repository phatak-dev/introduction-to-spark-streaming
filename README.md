This repository contains example code and sample data for *An Introduction to Real time Spark* session.
Follow the below steps to clone code and setup your machine.


## Prerequisites

* Java
* Maven 3
* Netcat

## 1. Tools

If you are linux, please make sure you have **nc** command. On windows, please install [ncat](http://nmap.org/ncat/).
This tool is required for socket examples.


## 2. Getting code

           git clone https://github.com/phatak-dev/introduction-to-spark-streaming


## 3. Build

        mvn clean install

### 4. Testing

On Linux, Run *nc* as follows 

     nc -l localhost 50050
     
then run the following command from code directory        
  
     java -cp target/spark-streaming.jar com.madhukaraphatak.sparktraining.streaming.WordCount local[2] localhost 50050
    

On Windows, first find out your ip address. Then run *netcat* as below      

    nectcat -l <ip-address> 50050

then run the following command from code directory         
  
    java -cp target\spark-streaming.jar com.madhukaraphatak.sparktraining.streaming.WordCount local[2] <ip-address> 50050   


## 5. Loading into an IDE

You can run all the examples from terminal. If you want to run from the IDE, follow the below steps


* IDEA 14

 Install [scala](https://plugins.jetbrains.com/plugin/?id=1347) plugin. Once plugin is loaded you can load it as [maven
 project](https://www.jetbrains.com/idea/help/importing-project-from-maven-model.html).


## 4. Up to date

Please pull before coming to the session to get the latest code.
