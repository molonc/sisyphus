#!/bin/bash
source /home/prod/miniconda3/bin/activate scpipeline
source /home/prod/sisyphus/scripts/sourceMe
export SLACK_BOT_TOKEN=xoxb-407675855762-2183580294480-JBhQxVVfDZPi5Phdi6hCOs5v
/home/prod/miniconda3/envs/scpipeline/bin/python /home/prod/sisyphus/workflows/run_qc.py \
> /home/prod/sisyphus/workflows/runAnalyses.out \
2> /home/prod/sisyphus/workflows/runAnalyses.err
