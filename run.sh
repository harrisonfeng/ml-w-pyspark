#!/bin/bash


SAMPLE_DATA=sample_data.csv
PYSPARK_SCRIPT=ml_w_pyspark.py

function usage() {
    echo " $0 <SPARK_HOME> "
}


function get_spark_home() {
    SPARK_HOME=$1
    if [ -z ${SPARK_HOME} ]; then
        usage
        exit 1
    fi
}


function place_data() {
    if [ ! -f /tmp/${SAMPLE_DATA} ]; then
        \cp ${PWD}/${SAMPLE_DATA} /tmp
    fi
}


function submit() {
    ${SPARK_HOME}/bin/spark-submit --master local --name "Data Processing" ${PYSPARK_SCRIPT}
}


function main() {

    get_spark_home $1
    place_data
    submit
}


main $1
