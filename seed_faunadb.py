import pandas as pd
from db_driver import FaunaWrapper

if __name__ == "__main__":
  client = FaunaWrapper()
  documents = client.get_documents_in_index('unique_full_news', 100000)
  print(len(documents.get('data')))
  # news_df = pd.read_csv('news.csv')
  # news_json = news_df.to_dict('records')

  # for news_item in news_json:
  #   try:
  #     client.create_document_in_collection('news', news_item)
  #   except Exception as e:
  #     print(e)

  # halts_df = pd.read_csv('halts.csv')
  # halts_json = halts_df.to_dict('records')
  # for halt_item in halts_json:
  #   try:
  #     halt_value = client.create_document_in_collection('halts', halt_item)
  #     print(halt_value)
  #   except Exception as e:
  #     print(e)

  # full_news_df = pd.read_csv('full_news.csv')
  # full_news_json = full_news_df.to_dict('records')
  # for news_item in full_news_json:
  #   try:
  #     client.create_document_in_collection('full_news', news_item)
  #   except Exception as e:
  #     print(e)
  #  "source": "ACCESSWIRE",
  #  "link_href": "/news/01-communique-announces-updated-ironcap-120000764.html",
  #  "link_text": "01 Communique Announces Updated IronCAP X v1.1 Encrypted Email Platform with Seamless Sending Mechanism and End-to-End Encryption",
  #  "ticker": "ONE.V"
  pass
