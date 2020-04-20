intradb_sync was developed to parallelize dowloading of data from intradb to the harvard NCF by using multiple download processes spread out using a slurm cluster. 

## Project setup
First, you must set up your project-specific workhorse .sh script. This is what allows you to integrate with slurm (though you can also run locally). See the `examples` directory. 

You may wish to modify `download_submitter.py` to point to this shell script.

In order to run, you must pass `download_submitter.py` a csv file. If this CSV file has a 'Scanner' column, you can filter by scanner. The only required field is 'Subject' or 'MRI ID'.

sample call:
`./download_submitter.py --subject-csv /ncf/hcp/code/intradb_sync/xnat_export-kastman_12_3_2019_11_56_1.csv --use-slurm --nprocs 12`


Call Chain: 

download_submitter.py -> download.sh (slurm job) -> download_resources.py (actual downloader)


Dependencies:

This software uses [yaxil](https://github.com/harvard-nrg/yaxil) as its xnat interface.
