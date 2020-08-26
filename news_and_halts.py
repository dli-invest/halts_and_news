import sys
import os
import argparse
import pandas as pd
import numpy as np
import requests
import os
import json
import time
from typing import Union
from cad_tickers.news import get_halts_resumption, scrap_news_for_ticker

def post_webhook_content(content: str):
  url = os.getenv('DISCORD_NEWS_WEBHOOK')
  data = {}
  #for all params, see https://discordapp.com/developers/docs/resources/webhook#execute-webhook
  data["content"] = f"```{content}```"

  result = requests.post(url, data=json.dumps(data), headers={"Content-Type": "application/json"})

  try:
      result.raise_for_status()
  except requests.exceptions.HTTPError as err:
      print(err)
  else:
      print("Payload delivered successfully, code {}.".format(result.status_code))

def post_webhook_embeds(embeds):
  url = os.getenv('DISCORD_NEWS_WEBHOOK')
  data = {}
  data["content"] = ''
  #for all params, see https://discordapp.com/developers/docs/resources/webhook#execute-webhook
  data["embeds"] = embeds
  result = requests.post(url, data=json.dumps(data), headers={"Content-Type": "application/json"})

  try:
      result.raise_for_status()
  except requests.exceptions.HTTPError as err:
      print(err)
  else:
      print("Payload delivered successfully, code {}.".format(result.status_code))

def drop_unnamed_columns(df: pd.DataFrame):
  df.drop(df.columns[df.columns.str.contains('unnamed', case = False)], axis = 1, inplace = True)

def get_halts():
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

    if diff_df.empty == False:
      content_str = diff_df.to_string(index=False)
      # move later, just return df
      for chunk in [content_str[i:i+2000] for i in range(0, len(content_str), 2000)]:
        post_webhook_content(chunk)
        time.sleep(2)
  else:
    halts_df = get_halts_resumption()
    drop_unnamed_columns(halts_df)
    halts_df.to_csv('halts.csv')

  # make html file, redeploy with github pages
  halts_df.to_html('halts.html')

def get_tickers():
  url = os.getenv('STOCK_API')
  r = requests.get(f"{url}/api/tickers-full")
  resp = r.json()
  data = resp.get('data')
  return data

def is_valid_news_item(news_item: dict):
  if news_item.get('source') == None and news_item.get('link_href') == '' and news_item.get('link_text') == '':
    return False
  return True

def make_embed_from_news_item(news_item: pd.Series):
  """
    Description:
      link_text gets mapped to description
      link_href is url 
      title is source
  """
  single_embed = format_news_item_for_embed(news_item)
  embeds = [
    single_embed
  ]
  return embeds
  # description can take 2000 characters

def format_news_item_for_embed(news_item: Union[np.ndarray,pd.Series]):
  y_base_url = 'https://finance.yahoo.com'
  if isinstance(news_item, np.ndarray):
    try:
      source, link_href, link_text, ticker = news_item
      embed_obj = {
        "description": link_text,
        "url": f"{y_base_url}/{link_href}",
        "title": f"{ticker} - {source}"
      }
      return embed_obj
    except Exception:
      return {}
  else:
    return {
      "description": news_item['link_text'],
      'url': f"{y_base_url}/{news_item['link_href']}",
      'title': f"{news_item['ticker']} - {news_item['source']}"
    }

def get_news():
  # fetch all the tickers from dashboard
  tickers = get_tickers()
  news_df = pd.DataFrame()
  # Load csv if exists
  news_file = 'news.csv'
  if os.path.exists(news_file):
    try:
      old_news_df = pd.read_csv(news_file)
    except Exception as e:
      old_news_df = pd.DataFrame()
  else:
    old_news_df = pd.DataFrame()
  for t in tickers:
    stock_news = scrap_news_for_ticker(t)
    # filter list
    stock_news = [i for i in stock_news if is_valid_news_item(i)] 
    if len(stock_news) == 0:
      continue
    news_df = news_df.append(stock_news, ignore_index=True)

  updated_news_df = pd.concat([old_news_df, news_df]) \
    .drop_duplicates(subset=['link_href', 'link_text'], keep=False) \
    .reset_index(drop=True)
  if updated_news_df.empty == False:
    for index, row in updated_news_df.iterrows():
      embeds = make_embed_from_news_item(row)
      post_webhook_embeds(embeds)
      time.sleep(2)
  
  news_df.to_csv('news.csv')
  updated_news_df.to_html('news.html')

if __name__ == "__main__":
  # Grab news for my stocks
  assert sys.version_info >= (3, 6)
  # grabbing all news for all stocks will be done in another script
  # no need to publish the results to github pages
  get_halts()
  get_news()
