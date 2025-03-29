FROM scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_alignment:v0.8.26
ADD . /app

RUN cp /app/split_bam.py /opt/conda/lib/python3.7/site-packages/single_cell/split_bam.py