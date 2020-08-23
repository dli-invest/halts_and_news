import sys
import os
import argparse
import pandas as pd
from cad_tickers.news import get_halts_resumption
# https://friendlyuser.github.io/cad_tickers_list/8/cse_8_2020.csv
# https://friendlyuser.github.io/cad_tickers_list/8/tsx_8_2020.csv

def download_csvs():
  # check if files exists
  # if not download all the csvs from my other github repo
  # eventually I have to grab the latest autogenerated csvs
  # could just duplicate the code over
  pass

def get_halts():
  pass 

def drop_unnamed_columns(df: pd.DataFrame):
  df.drop(df.columns[df.columns.str.contains('unnamed', case = False)],axis = 1, inplace = True)

if __name__ == "__main__":
  assert sys.version_info >= (3, 6)
  download_csvs()
  # customize file names with argparser
  halts_file = 'halts.csv'
  if os.path.exists(halts_file):
    old_halts_df = pd.read_csv(halts_file)
    halts_df = get_halts_resumption()
    new_halts_df = pd.concat([old_halts_df,halts_df]) \
      .drop_duplicates(subset=['Halts', 'Listing'], keep='first') \
      .reset_index(drop=True)
    drop_unnamed_columns(new_halts_df)
    new_halts_df.to_csv('halts.csv')
    # Find new rows and send discord message
    diff_df = new_halts_df \
      .merge(old_halts_df, how = 'outer' , indicator=True) \
      .loc[lambda x : x['_merge'] == 'left_only']
    drop_unnamed_columns(diff_df)
  else:
    halts_df = get_halts_resumption()
    drop_unnamed_columns(halts_df)
    halts_df.to_csv('halts.csv')

  # make html file, redeploy with github pages
  halts_df.to_html('halts.html')
