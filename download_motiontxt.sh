#!/usr/bin/env bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH --mem 8000 # memory
#SBATCH --account somerville_lab
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

scriptdir=`pwd -P`
#cd /ncf/hcp/data/intradb
cd /ncf/hcp/data/intradb_multiprocfix

# Add a staggered sleep
sleep $[ ( $RANDOM % 300 ) + 1 ]s
python "${scriptdir}"/download_resources.py \
    --project CCF_HCD_STG \
    --scantype tfMRI \
    --collections tfMRI_CARIT_PA_preproc tfMRI_CARIT_AP_preproc tfMRI_GUESSING_PA_preproc tfMRI_GUESSING_AP_preproc \
    --file-regex '.*txt$' \
    $@

