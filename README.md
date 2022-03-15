# NewYorker - Yelp Dataset

In order to create data pipeline and to process yelp data I used spark in conjunction with python. Used shell scripting for initializing sparkSession and to set the session's configurations as per the requirement. Python is used for reading input files, transforming input data and write final output.

  - **sparkSubmit.sh** : Wrapper for invoking spark session and to set required conf for that particular session
  - **main.py** : Transformation script written in python which handles operation like reading JSON files from input directory, transforming data as per the requirement and load final data in provided output directory in JSON format
  - **log4j.properties** : Store properties related to logging

## Quick Start

To start with JSON files processing please first pull the docker image by using below command :
```
docker pull ghcr.io/navchhabra/yelp-pyspark:latest
```
Once it's available run this command to make container running :
```
docker run -it --rm -p 4040:4040 -p 8888:8888 -v "${PWD}":/home/jovyan/work ghcr.io/navchhabra/yelp-pyspark:latest
```
Replace `${PWD}` with current working directory in which sparkSubmit.sh and main.py is avaialble.
Once yelp-pyspark container is up and running, we can start pyspark transformation.
To run the code first unzip `input.zip` and pass following command : 
```
sh sparkSubmit.sh -i <input_files_dir> -o <output_files_dir>
```
After processing final JSON result files can be find under `<output_files_dir>`
