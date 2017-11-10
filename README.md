# SEMS-Hadoop

SEMS-Hadoop is a program developed at the National Center for
Supercomputing Applications in collaboration with the Center for
Computational Biotechnology and Genomic Medicine to map epistatic gene
interactions utilizing MapReduce for scalable distributed computing. It uses
step-wise linear regression model selection to build a cumulative statistical
model of epistatic interactions between SNP's and phenotypes.

## Requirements

Maven for builds and packaging.

## Usage

A bash script is included to build, package, and run SEMS-Hadoop. Simply call

    ./run.sh

Modify run.sh to use different files and change file paths.

## Methodology

The idea for using MapReduce, even though it is slow on iterative algorithms,
was to take advantage of the fact that not everything had to be kept in memory.
SEMS-Hadoop splits each line of the input files and performs linear regression 
on the current model values and each regressor, mapping the regressors to the 
corresponding p value. Values in the model that are no longer significant are 
removed from the model and the model is updated.

Values are passed to the MapReduce jobs by adding configuration strings. These
are parsed and converted as each job is launched.