"""
Extract snapshots from list of revisions.

The output format is csv.
"""

from typing import Mapping
from datetime import datetime

def __parse_user(userdct: Mapping) -> Mapping:
    return {"id": int(userdct["id"]),
            "screen_name": userdct["screen_name"],
            "location": userdct["location"],
            "followers_count": int(userdct["followers_count"]),
            "statuses_count": int(userdct["statuses_count"])
            }



def cast_json(dct: Mapping) -> Mapping:
    res = {"id": int(dct["id"]),
           "full_text": dct["full_text"],
           "lang": dct['lang'],
           "created_at": dct['created_at'],
           "retweet_count": int(dct['retweet_count']),
           "favorite_count": int(dct['favorite_count']),
           "user": __parse_user(dct.get("user", {}))
           }

    return res
