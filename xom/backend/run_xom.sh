#!/bin/bash

file_name=last_run_number.txt
#for time in "2017-06-07 11:19:00" "2017-06-07 12:19:00" "2017-06-07 13:19:00"
for time in "2017-04-03 14:16:40" "2017-05-01 20:28:55" "2017-06-07 09:17:48" "2017-07-03 11:51:24" "2017-08-14 13:38:28" "2017-10-09 08:49:20" "2017-11-20 21:18:14" "2018-01-24 11:09:02"
do

    echo "here we print the time"
    echo $time
    #this is the starting run number
    last_run_number=`tail -1 $file_name`
    echo "the last run number: $last_run_number"

    #now lets query the data base and get the next run number within one hour
    run_info=`python run_database.py "$time"`
    
    #the run info is a tuple that we want to split into single entry to be given to processManger
    #run_info: tuple, run_info=('data_type', 'data_source', 'run_name', 'run_number', 'straxen_version')
    #first let's get rid of the parentheses in the run_info
    run_info_array=`echo $run_info | tr -d '()'`

    # now split the tuple: use the delimiter "," with IFS
    IFS=" " read -r  data_type data_source run_name run_number context_version <<<"$run_info"
    if [ -z "$context_version" ]
    then
	context_version=v1.0
	fi
    echo "the run number is: $run_number"
    # here we compare the old and new run number
    if [ $(($run_number-$last_run_number)) -ge 1 ]
    then
	echo "here is the difference between the run numbers: "
	echo $(($run_number-$last_run_number))
	#assign the outputfolder
	outputfolder=/scratch/midway2/mlotfi/$context_version/$run_number
	# here we run the process manager
	./processManager --dataType $data_type --source $data_source --runName $run_name --runId $run_number --outFolder $outputfolder
	
	# now lets update the file with this last run_number
	echo "$run_number" > $file_name
	# rsync the directory
	rsync -avz   /scratch/midway2/mlotfi/$context_version  xom@xe1t-offlinemon.lngs.infn.it:/home/xom/data/xom
	#now we can delete the whole directory, but only if the rsync is succefull
	if [[ $? -gt 0 ]]
	then
	    #send an email for failure of the rsync
	    echo "The run number: $run_number failed to sync with context version $context_version" |  mail -s "fail syncing the run $run_number" mlb20@nyu.edu
	    echo "There is problem syncing the run $run_number"
	else
	    rm -rf /scratch/midway2/mlotfi/$context_version/
	fi
	
	
    else
	echo "there are no files yet in the data base"
	echo "sleep 10 seconds and try again"
	sleep 10
    fi
done
# for testing purposes: we write again the run_number of the runs we are testing
echo "8423" > $file_name
