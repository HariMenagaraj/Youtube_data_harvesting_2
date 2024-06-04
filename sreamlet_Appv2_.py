
import requests
import pymongo
import sqlite3
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import isodate



# Data collection functions:

# Connecting the API using key to get data;
def get_connect_youtube_data_api():
    api_key = 'AIzaSyD5tRIFH_b7IhWLDMxXO0yYZkZ9xMMZ78Q'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = get_connect_youtube_data_api()

# Getting channel data;
def get_channel_data(channel_id):
    request = youtube.channels().list(
        part="contentDetails,snippet,statistics",
        id=channel_id
    )
    response = request.execute()

    if 'items' not in response:
        print(f"Response Error: {response}")
        raise KeyError("'items' key not found in the response.")

    for i in response['items']:
        channel_data = dict(
            Channel_Name=i['snippet']['title'],
            Channel_Id=i['id'],
            Subscription_Count=i['statistics']['subscriberCount'],
            Channel_Views=i['statistics']['viewCount'],
            Channel_Description=i['snippet']['description'],
            playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
        )
    return channel_data

# Collecting video ID by using channel ID;
def get_video_id(channel_id):
    list_of_video_id = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()

    if 'items' not in response:
        print(f"Response Error: {response}")
        raise KeyError("'items' key not found in the response.")

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page
        ).execute()

        if 'items' not in response1:
            print(f"Response Error: {response1}")
            raise KeyError("'items' key not found in the response.")

        for item in response1['items']:
            list_of_video_id.append(item['snippet']['resourceId']['videoId'])

        next_page = response1.get('nextPageToken')
        if next_page is None:
            break

    return list_of_video_id

# Getting video information like, comment, etc.;
def get_video_data(video_id_collection):
    video_id_datas = []
    for video_id in video_id_collection:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response = request.execute()

        if 'items' not in response:
            print(f"Response Error: {response}")
            continue

        for items in response['items']:
            data = dict(
                Channel_Id=items['snippet']['channelId'],
                channel_Name=items['snippet']['channelTitle'],
                Video_Id=items['id'],
                Video_Name=items['snippet']['title'],
                Video_Description=items['snippet']['description'],
                tags=items['snippet'].get('tags'),
                PublishedAt=items['snippet']['publishedAt'],
                View_Count=items['statistics']['viewCount'],
                Like_Count=items['statistics'].get('likeCount'),
                Favorite_Count=items['statistics']['favoriteCount'],
                Comment_Count=items['statistics'].get('commentCount'),
                Duration=items['contentDetails']['duration'],
                Thumbnail=items['snippet']['thumbnails']['default']['url'],
                Caption_Status=items['contentDetails']['caption'],
                Comments=items['statistics'].get('comments')
            )
            video_id_datas.append(data)
    return video_id_datas

# Getting comment information like an author and what the author wrote;
def get_comment_data(video_id_collection):
    comment_data_list = []
    for video_id in video_id_collection:
        try:
            requests = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100
            )
            response = requests.execute()

            if 'items' not in response:
                print(f"Response Error: {response}")
                continue

            for items in response['items']:
                comment_data = dict(
                    Comment_Id=items['id'],
                    video_id=items['snippet']['videoId'],
                    Comment_Text=items['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=items['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    published_at=items['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data_list.append(comment_data)
        except Exception as e:
            print(f"Error fetching comments for video ID {video_id}: {e}")
            continue
    return comment_data_list

# Getting playlist information from the playlist section on YouTube;
def get_playlist_data(channel_id):
    playlist_data_list = []
    request = youtube.playlists().list(
        part='snippet,contentDetails',
        channelId=channel_id,
        maxResults=50,
    )
    response = request.execute()

    if 'items' not in response:
        print(f"Response Error: {response}")
        raise KeyError("'items' key not found in the response.")

    for items in response['items']:
        data = dict(
            playlist_Id=items['id'],
            title=items['snippet']['title'],
            channel_Id=items['snippet']['channelId'],
            channel_Name=items['snippet']['channelTitle'],
            published_at=items['snippet']['publishedAt'],
            vide_count=items['contentDetails']['itemCount']
        )
        playlist_data_list.append(data)
    return playlist_data_list

# Connecting and uploading to MongoDB;
client = pymongo.MongoClient("mongodb+srv://sabarimenagaraj:otKANNGVActLc0eH@cluster0.ub6rep6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db_mongo = client['youtube_data']

# Gathering all data;
def get_all_youtube_data(channel_id):
    channel_data = get_channel_data(channel_id)
    playlist_data = get_playlist_data(channel_id)
    video_id = get_video_id(channel_id)
    video_data = get_video_data(video_id)
    comment_data = get_comment_data(video_id)

    collection = db_mongo['get_all_youtube_data']
    collection.insert_one({
        "channel_data": channel_data,
        "playlist_data": playlist_data,
        "video_data": video_data,
        "comment_data": comment_data
    })
    return "Data uploaded successfully"

# Uploading data to MongoDB;
def upload_data_to_mongo(channel_id):
    ch_id_data_list = []
    collection = db_mongo['get_all_youtube_data']
    for ch_data in collection.find({}, {'_id': 0, 'channel_data': 1}):
        ch_id_data_list.append(ch_data['channel_data']['Channel_Id'])

    if channel_id in ch_id_data_list:
        return "Given channel's data already exists"
    else:
        insert = get_all_youtube_data(channel_id)
        return insert


"""*************************************************************************************************************"""


# SQL connection and table creation functions :

# channel_table_create;

def channel_table_create():
    connection_db_sql = sqlite3.connect(database='youtube_data')

    cursor=connection_db_sql.cursor()

    drop_query = """DROP TABLE IF EXISTS Channel"""
    cursor.execute(drop_query)
    connection_db_sql.commit()

    try:
      create_query = """CREATE TABLE IF NOT EXISTS Channel(Channel_Name VARCHAR(60),
                                                        Channel_Id VARCHAR(60) PRIMARY KEY,
                                                        Subscription_Count BIGINT,
                                                        Channel_Views BIGINT,
                                                        Channel_Description TEXT,
                                                        playlist_id VARCHAR(60))"""
      cursor.execute(create_query)
      connection_db_sql.commit()


    except:
      print("Table alredy exists")


    ch_data_list = []
    db_mongo = client['youtube_data']
    collection = db_mongo['get_all_youtube_data']
    for ch_data in collection.find({},{'_id':0,'channel_data':1}):
      ch_data_list.append(ch_data['channel_data'])

    df = pd.DataFrame(ch_data_list)

    # data clening;

    df = df.where(pd.notnull(df), None)
    df['Channel_Name'] = df['Channel_Name'].fillna('No Channel Name')
    df['Subscription_Count'] = df['Subscription_Count'].fillna(0).astype(int)
    df['Channel_Views'] = df['Channel_Views'].fillna(0).astype(int)
    df['Channel_Description'] = df['Channel_Description'].fillna('No Description')
    df['playlist_id'] = df['playlist_id'].fillna('No Playlist ID')



    for index,row in df.iterrows():
      insert = """INSERT INTO Channel(Channel_Name,Channel_Id, Subscription_Count,
              Channel_Views, Channel_Description, playlist_id )
              VALUES(?,?,?,?,?,?)"""

      values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Channel_Description'],
                row['playlist_id'])
      try:
        cursor.execute(insert,values)
        connection_db_sql.commit()


      except:
        print("Already there")

    connection_db_sql.close()

# playlist_table_create;

def playlist_table_create():
    connection_db_sql = sqlite3.connect(database='youtube_data')

    cursor=connection_db_sql.cursor()

    drop_query = """DROP TABLE IF EXISTS Playlist"""
    cursor.execute(drop_query)
    connection_db_sql.commit()


    create_query = """CREATE TABLE IF NOT EXISTS Playlist
                      (playlist_Id VARCHAR(60) PRIMARY KEY,
                      title VARCHAR(100),
                      channel_Id VARCHAR(60),
                      channel_Name varchar(60),
                      published_at TIMESTAMP,
                      vide_count INT)"""


    cursor.execute(create_query)
    connection_db_sql.commit()


    pl_data_list_row = []
    pl_data_set = set(pl_data_list_row)
    pl_data_list = list(pl_data_set)
    db_mongo = client['youtube_data']
    collection = db_mongo['get_all_youtube_data']
    for pl_data in collection.find({},{'_id':0,'playlist_data':1}):
      for i in range(len(pl_data['playlist_data'])):
        pl_data_list_row.append(pl_data['playlist_data'][i])

    df1 = pd.DataFrame(pl_data_list)


    for index,row in df1.iterrows():
      insert = """INSERT INTO Playlist(playlist_Id,
                                      title,
                                      channel_Id,
                                      channel_Name,
                                      published_at,
                                      vide_count)
                                      VALUES(?,?,?,?,?,?)"""

      values = (row['playlist_Id'],
                row['title'],
                row['channel_Id'],
                row['channel_Name'],
                row['published_at'],
                row['vide_count'])

      cursor.execute(insert,values)
      connection_db_sql.commit()

    connection_db_sql.close()

# to avoid duration dataformate related errors;

def iso8601_duration_to_seconds(duration):
    try:
        return int(isodate.parse_duration(duration).total_seconds())
    except:
        return 0

# videos_table_create;

def videos_table_create():
  connection_db_sql = sqlite3.connect(database='youtube_data')

  cursor=connection_db_sql.cursor()

  drop_query = """DROP TABLE IF EXISTS Videos"""
  cursor.execute(drop_query)
  connection_db_sql.commit()



  create_query = """CREATE TABLE IF NOT EXISTS Videos
                    (Channel_Id VARCHAR(60),
                    channel_Name VARCHAR(50),
                    Video_Id VARCHAR(60),
                    Video_Name VARCHAR(100),
                    Video_Description TEXT,
                    tags TEXT,
                    PublishedAt TIMESTAMP,
                    View_Count BIGINT,
                    Like_Count BIGINT,
                    Favorite_Count INT,
                    Comment_Count BIGINT,
                    Duration INTERVAL,
                    Thumbnail VARCHAR(300),
                    Caption_Status VARCHAR(20),
                    Comments VARCHAR(300)
                    )"""


  cursor.execute(create_query)
  connection_db_sql.commit()


  vid_data_list = []
  db_mongo = client['youtube_data']
  collection = db_mongo['get_all_youtube_data']
  for vid_data in collection.find({},{'_id':0,'video_data':1}):
    for i in range(len(vid_data['video_data'])):
      vid_data_list.append(vid_data['video_data'][i])

  df2 = pd.DataFrame(vid_data_list)

  # data clening ;

  df2.dropna(subset=['Channel_Id', 'Video_Id', 'PublishedAt'])
  df2['Duration'] = df2['Duration'].apply(iso8601_duration_to_seconds)
  df2['channel_Name'] = df2['channel_Name'].str.strip()
  df2['Video_Name'] = df2['Video_Name'].str.strip()
  df2['tags'] = df2['tags'].str.strip()
  df2['Thumbnail'] = df2['Thumbnail'].str.strip()
  df2['Caption_Status'] = df2['Caption_Status'].str.strip()
  df2['Comments'] = df2['Comments'].str.strip()
  df2['channel_Name'] = df2['channel_Name'].str.title()
  df2['Video_Name'] = df2['Video_Name'].str.title()
  df2['Caption_Status'] = df2['Caption_Status'].str.lower()

  for index,row in df2.iterrows():
    insert = """INSERT INTO Videos(Channel_Id,
                                    channel_Name,
                                    Video_Id,
                                    Video_Name,
                                    Video_Description,
                                    tags,
                                    PublishedAt,
                                    View_Count,
                                    Like_Count,
                                    Favorite_Count,
                                    Comment_Count,
                                    Duration,
                                    Thumbnail,
                                    Caption_Status,
                                    Comments)
                                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""



    values = (row['Channel_Id'],
              row['channel_Name'],
              row['Video_Id'],
              row['Video_Name'],
              row['Video_Description'],
              row['tags'],
              row['PublishedAt'],
              row['View_Count'],
              row['Like_Count'],
              row['Favorite_Count'],
              row['Comment_Count'],
              row['Duration'],
              row['Thumbnail'],
              row['Caption_Status'],
              row['Comments'])


    cursor.execute(insert,values)
    connection_db_sql.commit()

  connection_db_sql.close()

# Comments_table_create;

def Comments_table_create():
  connection_db_sql = sqlite3.connect(database='youtube_data')

  cursor=connection_db_sql.cursor()

  drop_query = """DROP TABLE IF EXISTS Comments"""
  cursor.execute(drop_query)
  connection_db_sql.commit()


  create_query = """CREATE TABLE IF NOT EXISTS Comments
                                              (Comment_Id VARCHAR(60) PRIMARY KEY,
                                              Video_Id VARCHAR(100),
                                              Comment_Text TEXT,
                                              Comment_Author varchar(60),
                                              published_at DATETIME
                                              )"""

  cursor.execute(create_query)
  connection_db_sql.commit()

  cmt_data_list_row = []
  cmt_data_set = set(cmt_data_list_row)
  cmt_data_list = list(cmt_data_set)
  db_mongo = client['youtube_data']
  collection = db_mongo['get_all_youtube_data']
  for cmt_data in collection.find({},{'_id':0,'comment_data':1}):
    for i in range(len(cmt_data['comment_data'])):
      cmt_data_list_row.append(cmt_data['comment_data'][i])

  df3 = pd.DataFrame(cmt_data_list)


  for index,row in df3.iterrows():
    insert = """INSERT INTO Comments(Comment_Id,
                                      Video_Id,
                                      Comment_Text,
                                      Comment_Author,
                                      published_at)
                                      VALUES(?,?,?,?,?)"""

    values = (row['Comment_Id'],
              row['video_id'],
              row['Comment_Text'],
              row['Comment_Author'],
              row['published_at'])

    cursor.execute(insert,values)
    connection_db_sql.commit()

  connection_db_sql.close()

# method to create all tables at ones;

def create_all_tables():
  channel_table_create()
  playlist_table_create()
  videos_table_create()
  Comments_table_create()

  return "Tables created smoothly"

"""****************************************************************************************************************"""

# Display functions::

# Show channel table data;

def display_channel_table():
  ch_data_list = []
  db_mongo = client['youtube_data']
  collection = db_mongo['get_all_youtube_data']
  for ch_data in collection.find({},{'_id':0,'channel_data':1}):
    ch_data_list.append(ch_data['channel_data'])

  df_of_st = st.dataframe(ch_data_list)

  return df_of_st

# Show playlist table data;
def display_playlist_table():
  pl_data_list = []
  db_mongo = client['youtube_data']
  collection = db_mongo['get_all_youtube_data']
  for pl_data in collection.find({},{'_id':0,'playlist_data':1}):
    for i in range(len(pl_data['playlist_data'])):
      pl_data_list.append(pl_data['playlist_data'][i])

  df1_of_st = st.dataframe(pl_data_list)

  return df1_of_st

# Show videos table data;

def display_videos_table():
  vid_data_list = []
  db_mongo = client['youtube_data']
  collection = db_mongo['get_all_youtube_data']
  for vid_data in collection.find({},{'_id':0,'video_data':1}):
    for i in range(len(vid_data['video_data'])):
      vid_data_list.append(vid_data['video_data'][i])

  df2_of_st = st.dataframe(vid_data_list)

  return df2_of_st

# Show comments table data;
def display_comments_table():
  cmt_data_list = []
  db_mongo = client['youtube_data']
  collection = db_mongo['get_all_youtube_data']
  for cmt_data in collection.find({},{'_id':0,'comment_data':1}):
    for i in range(len(cmt_data['comment_data'])):
      cmt_data_list.append(cmt_data['comment_data'][i])

  df3_of_st = st.dataframe(cmt_data_list)

  return df3_of_st

"""****************************************************************************************************************"""

# Streamlit visuals :


with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption("SQL")
    st.caption("MongoDB")
    st.caption("Data Collection")
    st.caption("Python Scripting")
    st.caption("API Integration")
    st.caption("Data Visualization using Streamlit")
    st.caption("Data Management using MongoDB and SQL")

channel_id = st.text_input("Enter Channel ID")

if st.button("Collect and store data"):
    result = upload_data_to_mongo(channel_id)
    st.success(result)

if st.button("Transfer to SQL Database"):
    tables = create_all_tables()
    st.success(tables)

show_table = st.radio("SELECT THE TABLE", ("channel_table", "playlist_table", "videos_table", "comments_table"))

if show_table == "channel_table":
    display_channel_table()
elif show_table == "playlist_table":
    display_playlist_table()
elif show_table == "videos_table":
    display_videos_table()
elif show_table == "comments_table":
    display_comments_table()

"""****************************************************************************************************************"""

# SQL connection and questions ;

connection_db_sql = sqlite3.connect(database='youtube_data')
cursor=connection_db_sql.cursor()

# qustions;

questions = st.selectbox("SELECT YOUR QUSTIONS",("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are theircorresponding video names?",
                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))


# SQL query for 1st qustion

connection_db_sql = sqlite3.connect(database='youtube_data')
cursor=connection_db_sql.cursor()

if questions == "1. What are the names of all the videos and their corresponding channels?":
  query1 = """
              SELECT Video_Name AS videos,channel_name AS channel
              FROM Videos
              """
  cursor.execute(query1)
  connection_db_sql.commit()
  t1 = cursor.fetchall()
  connection_db_sql.close()
  df = pd.DataFrame(t1,columns = ["video title","channel name"])
  st.write(df)

# SQL query for 2nd qustion

elif questions == "2. Which channels have the most number of videos, and how many videos do they have?":
  query2 = """
              SELECT channel_name, COUNT(*) AS video_count
              FROM Videos
              GROUP BY channel_name
              ORDER BY video_count DESC
          """
  cursor.execute(query2)
  connection_db_sql.commit()
  t2 = cursor.fetchall()
  df2 = pd.DataFrame(t2, columns=["channel name", "video count"])
  st.write(df2)

# SQL query for 3rd qustion

elif questions == "3. What are the top 10 most viewed videos and their respective channels?":

  query3 = """SELECT Video_Name AS Video, View_Count AS views, channel_name AS channel
              FROM Videos
              ORDER BY Views DESC 
              LIMIT 10         
          """
  cursor.execute(query3)
  connection_db_sql.commit()
  t3 = cursor.fetchall()
  df3 = pd.DataFrame(t3, columns=["Video","views","Channel name"])
  st.write(df3)

# SQL query for 4th qustion

elif questions == "4. How many comments were made on each video, and what are theircorresponding video names?":

  query4 = """SELECT Comment_Count AS Comments, Video_Name AS Video
              FROM Videos
              ORDER BY Comment_Count DESC         
          """
  cursor.execute(query4)
  connection_db_sql.commit()
  t4 = cursor.fetchall()
  df4 = pd.DataFrame(t4, columns=["Comments","Video"])
  st.write(df4)

# SQL query for 5th qustion

elif questions == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":

  query5 = """SELECT Like_Count AS Likes, Video_Name AS Video, channel_name AS Channel
              FROM Videos
              ORDER BY Like_Count DESC 
              LIMIT 100        
          """
  cursor.execute(query5)
  connection_db_sql.commit()
  t5 = cursor.fetchall()
  df5 = pd.DataFrame(t5, columns=["Likes","Video","Channel"])
  st.write(df5)

# SQL query for 6th qustion

elif questions == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":

  query6 = """SELECT Like_Count AS Total_Likess, Video_Name AS Video
              FROM Videos
              ORDER BY Like_Count         
          """
  cursor.execute(query6)
  connection_db_sql.commit()
  t6 = cursor.fetchall()
  df6 = pd.DataFrame(t6, columns=["Total_Likess","Video"])
  st.write(df6)

# SQL query for 7th qustion

elif questions == "7. What is the total number of views for each channel, and what are their corresponding channel names?":

  query7 = """SELECT Channel_Views AS Total_Views, Channel_Name AS Channel
              FROM Channel       
          """
  cursor.execute(query7)
  connection_db_sql.commit()
  t7 = cursor.fetchall()
  df7 = pd.DataFrame(t7, columns=["Total_Views","Channel"])
  st.write(df7)

# SQL query for 8th qustion

elif questions == "8. What are the names of all the channels that have published videos in the year 2022?":

  query8 = """
            SELECT PublishedAt AS Year, Video_Name AS Video, channel_Name AS Channel
            FROM Videos
            WHERE CAST(PublishedAt AS DATETIME) BETWEEN '2022-01-01' AND '2022-12-31'
            ORDER BY Year 
          """
  cursor.execute(query8)
  connection_db_sql.commit()
  t8 = cursor.fetchall()
  df8 = pd.DataFrame(t8, columns=["Year","Video","Channel"])
  st.write(df8)

# SQL query for 9th qustion

elif questions == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":

  query9 = """
            SELECT AVG(Duration) AS average_duration_in_seconds, channel_Name AS Channel
            FROM Videos
            GROUP BY channel_Name
          """
  cursor.execute(query9)
  connection_db_sql.commit()
  t9 = cursor.fetchall()
  df9 = pd.DataFrame(t9, columns=["average_duration_in_seconds","Channel"])
  
  T91=[]
  for index,row in df9.iterrows():
      channel_title=row["Channel"]
      average_duration=row["average_duration_in_seconds"]
      average_duration_str=str(average_duration)
      T91.append(dict(average_duration_in_seconds=average_duration_str,Channel=channel_title))
  df91=pd.DataFrame(T91)
  st.write(df91)

# SQL query for 10th qustion

elif questions == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":

  query10 = """
            SELECT Video_Name AS Video, Comment_Count AS Comments, channel_Name AS Channel
            FROM Videos
            ORDER BY Comment_Count DESC
            LIMIT 200
          """
  cursor.execute(query10)
  connection_db_sql.commit()
  t10 = cursor.fetchall()
  df10 = pd.DataFrame(t10, columns=["Video","Comments","Channel"])
  st.write(df10)
