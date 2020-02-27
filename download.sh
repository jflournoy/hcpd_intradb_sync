#!/usr/bin/env bash
#
#SBATCH -p ncf # partition (queue)
#SBATCH --mem 4000 # memory
#SBATCH -t 3-2:00 # time (D-HH:MM)
#SBATCH -o logs/download_resources_%j_output.out
#SBATCH --mail-type=END # notifications for job done
set -aeuxo pipefail

# conda activate hcpl
if [ -z "${CONDA_DEFAULT_ENV-}" ]; then
  set +x
  echo "Please run \`conda activate hcpl\` and try again."
  set -x
  exit 1
fi
cd /ncf/hcp/data/intradb

# Add a staggered sleep
sleep $[ ( $RANDOM % 300 ) + 1 ]s
python /ncf/hcp/code/download_resources.py CCF_HCD_STG $@

