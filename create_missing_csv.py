import pandas as pd
import os, logging

def getDlDirs(datapath):
    dirs = [ f for f in os.listdir('/ncf/hcp/data/intradb_multiprocfix/') if os.path.isdir(datapath + f)]
    dirs = sorted(dirs)
    return(dirs)
def anti_join(x, y, on):
    """Return rows in x which are not present in y"""
    ans = pd.merge(left=x, right=y, how='left', indicator=True, on=on)
    ans = ans.loc[ans._merge == 'left_only', :].drop(columns='_merge')
    return ans

numeric_level = getattr(logging, 'INFO'.upper(), None)
logging.basicConfig(level=numeric_level)

datapath = '/ncf/hcp/data/intradb_multiprocfix/'
logging.info('Reading scan list, and dirs in %s.', datapath)
scanpd = pd.read_csv('HCP_DevScanList-Staged-2020_06_09.csv')
dldirs = getDlDirs(datapath)
dlpd = pd.DataFrame(data = {'Subject': dldirs})
dlpd['Subject'] = dlpd['Subject'].str.extract('(HCD.*?)_.*')

logging.info('Comparing scan list to dirs.')
missing = anti_join(scanpd, dlpd, on='Subject')
logging.info('Saving csv of missing scans. N = %i', len(missing.index))
missing.to_csv('missing_dls.csv', index=False)