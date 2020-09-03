import sys
import os
import argparse
import pandas as pd
import numpy as np
import requests
import os
import json
import time
from db_driver import FaunaWrapper
from typing import Union
from cad_tickers.news import get_halts_resumption, scrap_news_for_ticker


def post_webhook_content(content: str, embeds: list = None):
    url = os.getenv("DISCORD_NEWS_WEBHOOK")
    data = {}
    if content == "":
        data["content"] = ""
    else:
        # for all params, see https://discordapp.com/developers/docs/resources/webhook#execute-webhook
        data["content"] = f"```{content}```"
    if embeds is not None:
        data["embeds"] = embeds

    result = requests.post(
        url, data=json.dumps(data), headers={"Content-Type": "application/json"}
    )

    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
    else:
        print("Payload delivered successfully, code {}.".format(result.status_code))


def post_webhook_embeds(embeds):
    url = os.getenv("DISCORD_NEWS_WEBHOOK_ALL")
    data = {}
    data["content"] = ""
    # for all params, see https://discordapp.com/developers/docs/resources/webhook#execute-webhook
    data["embeds"] = embeds
    result = requests.post(
        url, data=json.dumps(data), headers={"Content-Type": "application/json"}
    )

    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
    else:
        print("Payload delivered successfully, code {}.".format(result.status_code))


def drop_unnamed_columns(df: pd.DataFrame):
    df.drop(
        df.columns[df.columns.str.contains("unnamed", case=False)], axis=1, inplace=True
    )


def get_halts():
    # customize file names with argparser
    halts_file = "halts.csv"
    client = FaunaWrapper()
    halts_cols = ["Halts", "Listing"]
    if os.path.exists(halts_file):
        old_halts_df = pd.read_csv(halts_file)
        drop_unnamed_columns(old_halts_df)
        halts_df = get_halts_resumption()
        merged_halts = pd.merge(
            halts_df, old_halts_df, on=halts_cols, how="left", indicator=True
        )
        merged_halts.dropna(inplace=True)

        valid_items = []
        # get the entries only in the left column, these are new
        new_halts_df = merged_halts.loc[merged_halts._merge == "left_only", halts_cols]
        fauna_list = new_halts_df.to_dict("records")
        for news_item in fauna_list:
            has_succeeded = client.create_document_in_collection("halts", news_item)
            if has_succeeded == True:
                valid_items.append(news_item)

        unseen_halts_df = pd.DataFrame(valid_items, columns=halts_cols)
        if unseen_halts_df.empty == False:
            content_str = unseen_halts_df.to_string(index=False)
            # move later, just return df
            for chunk in [
                content_str[i : i + 2000] for i in range(0, len(content_str), 2000)
            ]:
                post_webhook_content(chunk)
                time.sleep(2)
    else:
        halts_df = get_halts_resumption()
    halts_df.to_csv("halts.csv", index=False)

    # make html file, redeploy with github pages
    halts_df.to_html("halts.html", index=False)


def get_tickers():
    url = os.getenv("STOCK_API")
    r = requests.get(f"{url}/api/tickers-full")
    resp = r.json()
    data = resp.get("data")
    return data


def is_valid_news_item(news_item: dict):
    if (
        news_item.get("source") == None
        and news_item.get("link_href") == ""
        and news_item.get("link_text") == ""
    ):
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
    embeds = [single_embed]
    return embeds
    # description can take 2000 characters


def format_news_item_for_embed(news_item: Union[np.ndarray, pd.Series, dict]):
    y_base_url = "https://finance.yahoo.com"
    if isinstance(news_item, np.ndarray):
        try:
            source, link_href, link_text, ticker = news_item
            embed_obj = {
                "description": link_text,
                "url": f"{y_base_url}/{link_href}",
                "title": f"{ticker} - {source}",
            }
            return embed_obj
        except Exception as e:
            print(e)
            return {}
    elif isinstance(news_item, dict):
        embed_obj = {
            "description": news_item.get("link_text"),
            "url": f"{y_base_url}/{link_href}",
            "title": f"{ticker} - {source}",
        }
        return embed_obj
    else:
        return {
            "description": news_item["link_text"],
            "url": f"{y_base_url}/{news_item['link_href']}",
            "title": f"{news_item['ticker']} - {news_item['source']}",
        }


def get_news():
    # fetch all the tickers from dashboard
    tickers = get_tickers()
    news_df = pd.DataFrame()
    # Load csv if exists
    client = FaunaWrapper()
    news_file = "news.csv"
    df_cols = ["source", "link_href", "link_text", "ticker"]
    if os.path.exists(news_file):
        try:
            old_news_df = pd.read_csv(news_file)
        except Exception as e:
            old_news_df = pd.DataFrame()
    else:
        old_news_df = pd.DataFrame()

    # this is for my key tickers from the dash board, some be quick
    for t in tickers:
        stock_news = scrap_news_for_ticker(t)
        # filter list
        stock_news = [i for i in stock_news if is_valid_news_item(i)]
        if len(stock_news) == 0:
            continue
        news_df = news_df.append(stock_news, ignore_index=True)

    merged_news = pd.merge(news_df, old_news_df, on=df_cols, how="left", indicator=True)
    merged_news.dropna(inplace=True)

    valid_items = []
    # get the entries only in the left column, these are new
    updated_news_df = merged_news.loc[merged_news._merge == "left_only", df_cols]
    fauna_list = updated_news_df.to_dict("records")
    for news_item in fauna_list:
        has_succeeded = client.create_document_in_collection("news", news_item)
        if has_succeeded == True:
            valid_items.append(news_item)

    unseen_news_df = pd.DataFrame(valid_items, columns=df_cols)
    if unseen_news_df.empty == False:
        print(unseen_news_df)
        for index, row in unseen_news_df.iterrows():
            embeds = make_embed_from_news_item(row)
            post_webhook_content("", embeds)
            time.sleep(2)

    news_df.to_csv("news.csv", index=False)
    updated_news_df.to_html("news.html", index=False)


if __name__ == "__main__":
    # Grab news for my stocks
    assert sys.version_info >= (3, 6)
    # grabbing all news for all stocks will be done in another script
    # no need to publish the results to github pages
    get_halts()
    get_news()
