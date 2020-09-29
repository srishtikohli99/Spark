# SPARK

# `Setting up Spark for MAC OS`

## Requirements 

1. Anaconda 
2. Python 3.7 environment (doesn't work for 3.8 as of now)
3. Spark 
4. JDK 8
 
## Having installed all the requirements:

Run the following:

export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home

export SPARK_HOME=/Library/spark

export PATH=$JAVA_HOME/bin:$PATH

export PATH=$SPARK_HOME/bin:$PATH


Save these in ~/.bash_profile ans save

Running the commands will export these for only that bash instance

Working in conda env python 3.7


# `Running Spark`

### Running file

1. Activate the env
2. cd in repo
3. run spark-submit <filename>
 
### Run pyspark in shell

1. cd in Spark folder(where it is installed) /Library/spark
2. type `pyspark` in terminal
3. ready

 
