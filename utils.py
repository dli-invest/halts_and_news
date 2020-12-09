import requests
import json
import os


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

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')