import requests
import os
import sys
import pandas as pd
import numpy as np
import time
from news_and_halts import format_news_item_for_embed, is_valid_news_item, post_webhook_embeds, drop_unnamed_columns
from cad_tickers.news import scrap_news_for_ticker
from concurrent.futures import ThreadPoolExecutor

# the setrecursionlimit function is 
# used to modify the default recursion 
# limit set by python. Using this,  
# we can increase the recursion limit 
# to satisfy our needs 
sys.setrecursionlimit(10**6) 

def download_csvs():
  # check if files exists
  # if not download all the csvs from my other github repo
  # eventually I have to grab the latest autogenerated csvs
  # could just duplicate the code over
  url = 'https://friendlyuser.github.io/cad_tickers_list/8/cse_8_2020.csv'
  r = requests.get(url, allow_redirects=True)
  cse_file = 'cse.csv'
  if os.path.exists(cse_file) == False :
    with open(cse_file, 'wb') as file_:
      file_.write(r.content)

  url = 'https://friendlyuser.github.io/cad_tickers_list/8/tsx_8_2020.csv'
  r = requests.get(url, allow_redirects=True)
  tsx_file = 'tsx.csv'
  if os.path.exists(tsx_file) == False :
    with open(tsx_file, 'wb') as file_:
      file_.write(r.content)
  pass

def get_tickers():
  # grab tsx data
  tsx_df = pd.read_csv('tsx.csv')
  tsx_df = tsx_df[['Ex.', 'Ticker']]
  ytickers_series = tsx_df.apply(tsx_ticker_to_yahoo, axis=1)
  ytickers_series = ytickers_series.drop_duplicates(keep='last')
  tsx_tickers = ytickers_series.tolist()

  cse_df = pd.read_csv('cse.csv')
  cse_df = cse_df[['Symbol']]
  ytickers_series = cse_df.apply(cse_ticker_to_yahoo, axis=1)
  ytickers_series = ytickers_series.drop_duplicates(keep='last')
  cse_tickers = ytickers_series.tolist()

  ytickers = [*tsx_tickers, *cse_tickers]
  return ytickers

def cse_ticker_to_yahoo(row: pd.Series)-> str:
  ticker = row['Symbol']
  return f"{ticker}.CN"

def tsx_ticker_to_yahoo(row: pd.Series)-> str:
  """
    Parameters:
      ticker: ticker from pandas dataframe from cad_tickers
      exchange: what exchange the ticker is for
    Returns:
  """
  ticker = row['Ticker']
  exchange = row['Ex.']
  # 1min, 5min, 15min, 30min, 60min, daily, weekly, monthly
  switcher = {
    "TSXV":   "V",
    "TSX":     "TO"
  }
  yahoo_ex = switcher.get(exchange, "TSXV")
  return f"{ticker}.{yahoo_ex}"


def generate_news_items(tickers: str):
  # at most 10 embeds
  # each news item had at most 3
  for chunked_tickers in np.array_split(tickers, 3):
    raw_news = []
    for ticker in chunked_tickers:
      news_items = scrap_news_for_ticker(ticker)
      raw_news.append(news_items)
      time.sleep(0.67)
    yield raw_news


if __name__ == "__main__":
  # Grab news for my stocks
  assert sys.version_info >= (3, 6)
  # grabbing all news for all stocks will be done in another script
  # no need to publish the results to github pages
  download_csvs()
  tickers = get_tickers()[0:3]
  # single threaded approach
  # raw_news = []
  # for ticker in tickers:
  #   news_items = scrap_news_for_ticker(ticker)
  #   raw_news.append(news_items)
  # flatten list
  full_news_list = []
  flatten = lambda l: [item for sublist in l for item in sublist]
  fnews_file = 'full_news.csv'
  df_cols = ['source','link_href','link_text','ticker']
  if os.path.exists(fnews_file):
    old_news_df = pd.read_csv(fnews_file)
  else:
    old_news_df = pd.DataFrame(columns=df_cols)
  # Using https://stackoverflow.com/questions/28901683/pandas-get-rows-which-are-not-in-other-dataframe
  for raw_news in generate_news_items(tickers):
    flat_news = flatten(raw_news)
    full_news_list.append(flat_news)
    # remove empty news articles
    valid_news = [i for i in flat_news if is_valid_news_item(i)]
    if old_news_df.empty == False:
      full_news_flat = flatten(full_news_list)
      # remove unnamed columns
      temp_news_df = pd.DataFrame(full_news_flat, columns=df_cols)
      common = old_news_df.merge(temp_news_df,
        on=df_cols)
      unseen_news = old_news_df[(~old_news_df.link_href.isin(common.link_href))&(~old_news_df.link_text.isin(common.link_text))]
      # if this works, condense it
      if len(unseen_news) > 0:
        embeds_np = np.apply_along_axis(format_news_item_for_embed, axis=1, arr=unseen_news)
        embeds = embeds_np.tolist()
        post_webhook_embeds(embeds)
    else:
      if len(valid_news) > 0:
        embeds_np = np.apply_along_axis(format_news_item_for_embed, axis=1, arr=valid_news)
        embeds = embeds_np.tolist()
        post_webhook_embeds(embeds)
  condensed_list = flatten(full_news_list)
  save_df = pd.DataFrame(condensed_list, columns=df_cols)
  save_df.to_csv(fnews_file)
    # news_df = pd.DataFrame(valid_news)
    # get old news df from file
  # fnews_file = 'full_news.csv'
  # if os.path.exists(fnews_file):
  #   old_news_df = pd.read_csv(fnews_file)
  # else:
  #   old_news_df = pd.DataFrame()
  # updated_news_df = pd.concat([old_news_df, news_df]) \
  #   .drop_duplicates(subset=['link_href', 'link_text', 'ticker'], keep='first') \
  #   .reset_index(drop=True)
  # drop_unnamed_columns(updated_news_df)
  # if updated_news_df.empty == False:
  #   # randomly compute best chunk
  #   for chunk_array in np.array_split(updated_news_df, 6):
  #     embeds_np = np.apply_along_axis(format_news_item_for_embed, axis=1, arr=chunk_array)
  #     embeds = embeds_np.tolist()
  #     post_webhook_embeds(embeds)
  #     time.sleep(2)

  # news_df.to_csv(fnews_file)
