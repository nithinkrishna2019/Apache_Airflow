import pandas as pd
import s3fs
from datetime import datetime
from io import StringIO
import boto3

def run_twitter_etl():

    s3 = boto3.client('s3')

    bucket_name = "aws-glue-s3-bucket"
    file_key = "Apache_Airflow_PJ/tweets.csv"
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')
    tweets_csv = pd.read_csv(StringIO(csv_content), header=0)

    # s3.delete_object(Bucket=bucket_name, Key=file_key)
    # print(f"File deleted from s3://{bucket_name}/{file_key}")

    
    # tweets_csv = pd.read_csv(r"c:\Users\z033876\OneDrive - Alliance\Desktop\MY_Learning\Airflow_Apache\tweets.csv", header=0)

    tweets_csv = tweets_csv.drop(columns=["latitude", "longitude","country","id"],axis=1)

    tweets_csv["ID"] = range(1, len(tweets_csv) + 1) #adding a meaningful ID column from 1

    tweets_csv["date_time"] = pd.to_datetime(tweets_csv["date_time"], format="%d-%m-%Y %H:%M")

    language_map = {
        'en': 'English',
        'tl': 'Tagalog',
        'cy': 'Welsh',
        'und': 'Undefined (Unknown)',
        'in': 'Indonesian',
        'es': 'Spanish',
        'ht': 'Haitian Creole',
        'pt': 'Portuguese',
        'ro': 'Romanian',
        'lt': 'Lithuanian',
        'nl': 'Dutch',
        'et': 'Estonian',
        'fr': 'French',
        'de': 'German',
        'ja': 'Japanese',
        'da': 'Danish',
        'sv': 'Swedish',
        'hi': 'Hindi',
        'it': 'Italian',
        'eu': 'Basque',
        'hu': 'Hungarian',
        'fi': 'Finnish',
        'is': 'Icelandic',
        'no': 'Norwegian',
        'tr': 'Turkish',
        'sl': 'Slovenian',
        'pl': 'Polish',
        'cs': 'Czech',
        'ko': 'Korean',
        'ru': 'Russian',
        'ar': 'Arabic',
        'lv': 'Latvian'
    }

    tweets_csv["language"] = tweets_csv["language"].map(language_map) 
    #map checks each value in the column
    #if the key exist in language_map it replaces with the corresponding values

    tweets_csv["text_length"] = tweets_csv["content"].map(len)

    english_tweets=tweets_csv[tweets_csv["language"]=="English"] #filtering only english tweets

    #english_tweets = english_tweets.sort_values(by=["number_of_likes","number_of_shares"], ascending=[False,False])

    popular_tweets = english_tweets[
        (english_tweets["number_of_likes"] > 100000) & (english_tweets["number_of_shares"] > 100000)
    ]

    csv_buffer = StringIO()

    english_tweets.to_csv(csv_buffer, index=False)

    file_key_target="Apache_Airflow_PJ/Transformed/english_tweets.csv"

    s3.put_object(Bucket=bucket_name, Key=file_key_target, Body=csv_buffer.getvalue())

    print(f"File successfully uploaded to s3://{bucket_name}/{file_key}")