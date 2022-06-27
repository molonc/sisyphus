#!/bin/bash
source /home/prod/miniconda3/bin/activate scpipeline
source /home/prod/sisyphus/scripts/sourceMe
/home/prod/miniconda3/envs/scpipeline/bin/python /home/prod/sisyphus/scripts/cleaning_tantalus.py > /home/prod/cleaning.out 2> /home/prod/sisyphus/workflows/cleaning.err

