"""
Extract snapshots from list of revisions.

The output format is csv.
"""

from typing import Mapping
from datetime import datetime


# {
#   "created_at":"Tue Jan 21 22:45:27 +0000 2020",
#   "id":1219752899636613121,
#   "id_str":"1219752899636613121",
#   "full_text":"RT @AnneKPIX: @CDC has activated its emergency operations center. \nThey expect more US cases.\n#coronavirus",
#   "truncated":false,
#   "display_text_range":[
#      0,
#      106
#   ],
#   "entities":{
#      "hashtags":[
#         {
#            "text":"coronavirus",
#            "indices":[
#               94,
#               106
#            ]
#         }
#      ],
#      "symbols":[
#         
#      ],
#      "user_mentions":[
#         {
#            "screen_name":"AnneKPIX",
#            "name":"Anne Makovec",
#            "id":18650514,
#            "id_str":"18650514",
#            "indices":[
#               3,
#               12
#            ]
#         },
#         {
#            "screen_name":"cdc",
#            "name":"This is not the CDC",
#            "id":1532281,
#            "id_str":"1532281",
#            "indices":[
#               14,
#               18
#            ]
#         }
#      ],
#      "urls":[
#         
#      ]
#   },
#   "source":"<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>",
#   "in_reply_to_status_id":null,
#   "in_reply_to_status_id_str":null,
#   "in_reply_to_user_id":null,
#   "in_reply_to_user_id_str":null,
#   "in_reply_to_screen_name":null,
#   "user":{
#      "id":1110906564158869505,
#      "id_str":"1110906564158869505",
#      "name":"El Abuelo Huerco. \ud83c\uddf2\ud83c\uddfd #VibrarBonito.",
#      "screen_name":"Huerconetzin",
#      "location":"M\u00e9xico \ud83c\uddf2\ud83c\uddfd ",
#      "description":"Primero que nada amigo, luego escritor, poeta, tuittero de tiempo completo y lector asiduo de novelas, s\u00edgueme y hablemos de todo, y de nada. #GraciasALaVida",
#      "url":null,
#      "entities":{
#         "description":{
#            "urls":[
#               
#            ]
#         }
#      },
#      "protected":false,
#      "followers_count":420,
#      "friends_count":2034,
#      "listed_count":1,
#      "created_at":"Wed Mar 27 14:08:59 +0000 2019",
#      "favourites_count":18835,
#      "utc_offset":null,
#      "time_zone":null,
#      "geo_enabled":false,
#      "verified":false,
#      "statuses_count":16075,
#      "lang":null,
#      "contributors_enabled":false,
#      "is_translator":false,
#      "is_translation_enabled":false,
#      "profile_background_color":"000000",
#      "profile_background_image_url":"http://abs.twimg.com/images/themes/theme1/bg.png",
#      "profile_background_image_url_https":"https://abs.twimg.com/images/themes/theme1/bg.png",
#      "profile_background_tile":false,
#      "profile_image_url":"http://pbs.twimg.com/profile_images/1365674573799251968/qTyEQ5s6_normal.jpg",
#      "profile_image_url_https":"https://pbs.twimg.com/profile_images/1365674573799251968/qTyEQ5s6_normal.jpg",
#      "profile_banner_url":"https://pbs.twimg.com/profile_banners/1110906564158869505/1614559431",
#      "profile_image_extensions_alt_text":null,
#      "profile_banner_extensions_alt_text":null,
#      "profile_link_color":"981CEB",
#      "profile_sidebar_border_color":"000000",
#      "profile_sidebar_fill_color":"000000",
#      "profile_text_color":"000000",
#      "profile_use_background_image":false,
#      "has_extended_profile":false,
#      "default_profile":false,
#      "default_profile_image":false,
#      "following":false,
#      "follow_request_sent":false,
#      "notifications":false,
#      "translator_type":"none"
#   },
#   "geo":null,
#   "coordinates":null,
#   "place":null,
#   "contributors":null,
#   "retweeted_status":{
#      "created_at":"Tue Jan 21 19:09:49 +0000 2020",
#      "id":1219698632217116673,
#      "id_str":"1219698632217116673",
#      "full_text":"@CDC has activated its emergency operations center. \nThey expect more US cases.\n#coronavirus",
#      "truncated":false,
#      "display_text_range":[
#         0,
#         92
#      ],
#      "entities":{
#         "hashtags":[
#            {
#               "text":"coronavirus",
#               "indices":[
#                  80,
#                  92
#               ]
#            }
#         ],
#         "symbols":[
#            
#         ],
#         "user_mentions":[
#            {
#               "screen_name":"cdc",
#               "name":"This is not the CDC",
#               "id":1532281,
#               "id_str":"1532281",
#               "indices":[
#                  0,
#                  4
#               ]
#            }
#         ],
#         "urls":[
#            
#         ]
#      },
#      "source":"<a href=\"https://mobile.twitter.com\" rel=\"nofollow\">Twitter Web App</a>",
#      "in_reply_to_status_id":null,
#      "in_reply_to_status_id_str":null,
#      "in_reply_to_user_id":1532281,
#      "in_reply_to_user_id_str":"1532281",
#      "in_reply_to_screen_name":"cdc",
#      "user":{
#         "id":18650514,
#         "id_str":"18650514",
#         "name":"Anne Makovec",
#         "screen_name":"AnneKPIX",
#         "location":"San Francisco, CA",
#         "description":"TV News Reporter, animal lover, city dweller, Midwestern girl Californified.",
#         "url":"https://t.co/uIAHlWC0ek",
#         "entities":{
#            "url":{
#               "urls":[
#                  {
#                     "url":"https://t.co/uIAHlWC0ek",
#                     "expanded_url":"http://www.cbssf.com",
#                     "display_url":"cbssf.com",
#                     "indices":[
#                        0,
#                        23
#                     ]
#                  }
#               ]
#            },
#            "description":{
#               "urls":[
#                  
#               ]
#            }
#         },
#         "protected":false,
#         "followers_count":2492,
#         "friends_count":1773,
#         "listed_count":141,
#         "created_at":"Mon Jan 05 23:07:57 +0000 2009",
#         "favourites_count":3670,
#         "utc_offset":null,
#         "time_zone":null,
#         "geo_enabled":true,
#         "verified":true,
#         "statuses_count":4032,
#         "lang":null,
#         "contributors_enabled":false,
#         "is_translator":false,
#         "is_translation_enabled":false,
#         "profile_background_color":"642D8B",
#         "profile_background_image_url":"http://abs.twimg.com/images/themes/theme10/bg.gif",
#         "profile_background_image_url_https":"https://abs.twimg.com/images/themes/theme10/bg.gif",
#         "profile_background_tile":true,
#         "profile_image_url":"http://pbs.twimg.com/profile_images/1177213612466663430/zPHm581I_normal.jpg",
#         "profile_image_url_https":"https://pbs.twimg.com/profile_images/1177213612466663430/zPHm581I_normal.jpg",
#         "profile_banner_url":"https://pbs.twimg.com/profile_banners/18650514/1398628438",
#         "profile_image_extensions_alt_text":null,
#         "profile_banner_extensions_alt_text":null,
#         "profile_link_color":"0091FF",
#         "profile_sidebar_border_color":"65B0DA",
#         "profile_sidebar_fill_color":"7AC3EE",
#         "profile_text_color":"3D1957",
#         "profile_use_background_image":true,
#         "has_extended_profile":false,
#         "default_profile":false,
#         "default_profile_image":false,
#         "following":false,
#         "follow_request_sent":false,
#         "notifications":false,
#         "translator_type":"none"
#      },
#      "geo":null,
#      "coordinates":null,
#      "place":null,
#      "contributors":null,
#      "is_quote_status":false,
#      "retweet_count":245,
#      "favorite_count":567,
#      "favorited":false,
#      "retweeted":false,
#      "lang":"en"
#   },
#   "is_quote_status":false,
#   "retweet_count":245,
#   "favorite_count":0,
#   "favorited":false,
#   "retweeted":false,
#   "lang":"en"
# }

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
