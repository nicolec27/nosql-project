from pymongo import MongoClient
import csv
from datetime import datetime, tzinfo, timezone

# {"reviews.username" : "Cristina M"}
# { "reviews.date": { $gt: ISODate("2016-01-01"), $lt: ISODate("2017-01-01") } }

client = MongoClient('mongodb+srv://nchan14:lKdPdu2hFs3gayN2@cluster0.flq3uru.mongodb.net/')

q2 = [
    {
        '$match': {
            'reviews.date': {
                '$gt': datetime(2016, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 
                '$lt': datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            }
        }
    },
    {
        '$group': {
            '_id': '$name', 
            'avgRating': {
                '$avg': '$reviews.rating'
            },
            'reviews': {
                '$push': '$reviews.text'
            }
        }
    },
    {
        '$unwind': '$reviews'
    },
    {
        "$addFields": {
            "colors": {
                "$cond": {
                    "if": {
                        "$regexMatch": {
                            "input": "$reviews",
                            "regex": "\\b(black|blue|red|green|tan|gray|grey)\\b",
                            "options": "i"
                        }
                    },
                    "then": {
                        "$regexFind": {
                            "input": "$reviews",
                            "regex": "\\b(black|blue|red|green|tan|gray|grey)\\b",
                            "options": "i"
                        }
                    },
                    "else": "no color"
                }
            }
        }
    },
    {
        "$match": {
            "colors": { 
                "$ne": "no color" 
            }
        }
    },
    {
        '$project': {
            '_id': 1,
            'avgRating': 1,
            'reviews': 1,
            'colors': "$colors.match",
            # 'color': {'$last': '$col'},
        }
    },
    {
        '$sort': {
            'avgRating': -1
        }  
    }
]

cur = client['reviews']['sample'].aggregate(q2)
fields = ['product', 'avgRating', 'reviews', 'color']

with open('output2.csv', 'w', encoding="utf-8", newline='') as f:
    w = csv.writer(f)
    w.writerow(fields)
    for doc in cur: 
        w.writerow(doc.values())

q3 = [
    {
        '$group': {
            '_id': '$reviews.username', 
            # 'totalHelpful': {'$sum': '$reviews.numHelpful'},
            'reviews': {
                '$push': {
                    'total': '$reviews.numHelpful',
                    'name': '$name',
                    'txt': '$reviews.text'
                }
            }
        }
    },
    {
        '$unwind': '$reviews'
    },
    {
        '$addFields': {
            'techWords': {
                '$size': {
                    '$regexFindAll': {
                        'input': '$reviews.txt',
                        'regex': 'screen|feature|quality|light|display|sound|device|play|app|camera|microphone|tablet|echo|kindle',
                        'options': 'i' 
                    }
                }
            }
        }
    },
    {
        '$match': {
            'reviews.total': {'$gte': 0}
        }  
    },
    {
        '$project': {
            '_id': 1,
            'total': '$reviews.total',
            'name': '$reviews.name',
            'text': '$reviews.txt',
            'techWords': 1
        }
    },
    {
        '$sort': {'total': -1}  
    }
]



cur2 = client['reviews']['sample'].aggregate(q3)
fields2 = ['username', 'techWords', 'numHelpful', 'productName', 'review']

with open('output3.csv', 'w', encoding="utf-8", newline='') as f:
    w = csv.writer(f)
    w.writerow(fields2)
    for doc in cur2: 
        w.writerow(doc.values())


# print(result)