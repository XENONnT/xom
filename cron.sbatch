#!/bin/bash

#SBATCH --time=00:05:00
#SBATCH --output=cron.log
#SBATCH --open-mode=append
#SBATCH --account=cron-account
#SBATCH --partition=cron
#SBATCH --qos=cron

# Specify a valid Cron string for the schedule. This specifies that
SCHEDULE='* * * * *'

# Here is an example of a simple command that prints the host name and
# the date and time.

echo "running XOM manager on $(hostname) at $(date)."
source /cvmfs/xenon.opensciencegrid.org/releases/nT/2024.01.1/setup.sh
#source /cvmfs/xenon.opensciencegrid.org/releases/nT/2023.07.1/setup.sh
python source/manager.py --prefix test20240101_

# This schedules the next run.
sbatch --quiet --begin=$(next-cron-time "$SCHEDULE") cron.sbatch
