#!/usr/bin/env bash
# 
#use: 
#
#SBATCH -p ncf # partition (queue)
#SBATCH --mem 4000 # memory
#SBATCH -t 3-2:00 # time (D-HH:MM)
#SBATCH -o logs/download_resources_%j_output.out
#SBATCH --mail-type=END # notifications for job done
set -aeuxo pipefail

DB_LOCATION=/ncf/hcp/data/intradb
CODEPATH=/ncf/hcp/code/intradb_sync/ 


pushd $DB_LOCATION

# you may want to use CCF_HCD_STG or CCH_HCA_STG 
#depending on whether you're looking to download from aging or developmental datasets

python $CODEPATH/download_resources.py \
  --project CCF_HCD_STG \
  --collections "Structural_preproc" "Diffusion_unproc" "mbPCASLhr_unproc"\
  --subjects  $@
