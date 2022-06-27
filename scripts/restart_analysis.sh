#!/bin/bash
source /home/prod/miniconda3/bin/activate scpipeline
source /home/prod/sisyphus/scripts/sourceMe
analysis=$1
echo $analysis
#echo ~/saltant/singlecelllogs/pipeline/analysis_$1/hmmcopy