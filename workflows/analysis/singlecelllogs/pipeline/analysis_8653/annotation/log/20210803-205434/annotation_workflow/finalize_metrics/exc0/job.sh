set -e
set -o pipefail

clean_up () {
  echo "clean_up task executed"
  find /mnt/datadrive/$AZ_BATCH_TASK_WORKING_DIR/ -xtype l -delete
  exit 0
}
trap clean_up EXIT
mkdir -p /mnt/datadrive/$AZ_BATCH_TASK_WORKING_DIR/

cd /mnt/datadrive/$AZ_BATCH_TASK_WORKING_DIR/

docker run -w $PWD -v $PWD:$PWD --rm -v /datadrive:/datadrive -v /mnt:/mnt -v /refdata:/refdata scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_annotation:v0.8.0 pypeliner_delegate $AZ_BATCH_TASK_WORKING_DIR/job.pickle $AZ_BATCH_TASK_WORKING_DIR/job_result.pickle
sg docker -c "docker run -v /mnt/datadrive:/mnt/datadrive -w /mnt/datadrive/$AZ_BATCH_TASK_WORKING_DIR continuumio/miniconda find . -xtype l -delete"
sg docker -c "docker run -v /mnt/datadrive:/mnt/datadrive -w /mnt/datadrive/$AZ_BATCH_TASK_WORKING_DIR continuumio/miniconda find . -xtype f -delete"

wait