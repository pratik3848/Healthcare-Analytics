################################################################
# Developed By:                                                #
# Developed Date:                                              # 
# Script Name:                                                 #
# PURPOSE: Master Script to run the entire project end to end. #
################################################################

PROJ_FOLDER="/home/${USER}/PrescriberAnalytics/src/main/python"

### Call the copy_to_hdfs wrapper to copy the input vendor files from Local to HDFS..
printf "\nCalling copy_files_local_to_hdfs.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ... \n"
${PROJ_FOLDER}/bin/copy_files_local_to_hdfs.ksh
printf "Executing copy_files_local_to_hdfs.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

### Call below wrapper to delete HDFS Paths.
printf "Calling delete_hdfs_output_paths.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ... \n"
${PROJ_FOLDER}/bin/delete_hdfs_output_paths.ksh
printf "Executing delete_hdfs_output_paths.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

### Call below Spark Job to extract Fact and City Files (This covers Part 3 as well)
printf "Calling run_presc_pipeline.py at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
spark3-submit --master yarn  run_presc_pipeline.py
printf "Executing run_presc_pipeline.py is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

### Part 2
### Call below script to copy files from HDFS to local.
printf "Calling copy_files_hdfs_to_local.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
${PROJ_FOLDER}/bin/copy_files_hdfs_to_local.ksh
printf "Executing copy_files_hdfs_to_local.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

### Call below script to copy files to S3.
printf "Calling copy_files_to_s3.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
${PROJ_FOLDER}/bin/copy_files_to_s3.ksh
printf "Executing copy_files_to_s3.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

