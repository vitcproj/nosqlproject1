from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection
def Api_connect():
    Api_Id="AIzaSyANPdDfGJv18dk9WurUeVUTrlvOoHeRU0E"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()

def get_channel_info(channel_id):
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response = request.execute()

    for i in range(0,len(response["items"])):
        data = dict(
                    Channel_Name = response["items"][i]["snippet"]["title"],
                    Channel_Id = response["items"][i]["id"],
                    Subscription_Count= response["items"][i]["statistics"]["subscriberCount"],
                    Views = response["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response["items"][i]["snippet"]["description"],
                    Playlist_Id = response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data
    


### channel links

### zebrazone - "UC6PIKnoGw7rJnND6_jdoo5w"
### micset    - "UC5EQWvy59VeHPJz8mDALPxg"
### sameeha marium - "UCaJclHt8WiaQjbv9DY2gKjg"
### irfans view - "UCnjU1FHmao9YNfPzE039YTw"
### vj siddhu vlogs - "UCJcCB-QYPIBcbKcBQOTwhiA"
### jk - "UC_HZY9d5wJ-MEiuq6vhx8hg"
### jedcal - "UCpjLVja-gdCtElYvMmvYvYQ"

#get playlist ids

def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data


#get video ids
def get_channel_videos(channel_id):
    video_ids=[]
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information


client = pymongo.MongoClient("mongodb+srv://vikramviki1027:mirunalini100L@cluster0.jsiqqmd.mongodb.net/?retryWrites=true&w=majority")
db = client["youtube_project"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    return "upload completed successfully"

import psycopg2

def channels_table():
    try:
        mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="mirunalini100L",
            database="test1",
            port="5432"
        )
        cursor = mydb.cursor()

        create_query = '''CREATE TABLE IF NOT EXISTS channels (
                            Channel_Name VARCHAR(150),
                            Channel_Id VARCHAR(100) PRIMARY KEY,
                            Subscription_Count INT,
                            Views BIGINT,
                            Total_Videos INT,
                            Channel_Description TEXT,
                            Playlist_Id VARCHAR(100)
                        )'''
        
        cursor.execute(create_query)
        mydb.commit()

        ch_list = []
        db = client["youtube_project"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
            ch_list.append(ch_data["channel_information"])
        df = pd.DataFrame(ch_list)

        insert_query = '''INSERT INTO channels (
                            Channel_Name,
                            Channel_Id,
                            Subscription_Count,
                            Views,
                            Total_Videos,
                            Channel_Description,
                            Playlist_Id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''

        for index, row in df.iterrows():
            values = (
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id']
            )

            try:
                cursor.execute(insert_query, values)
                mydb.commit()
            except psycopg2.IntegrityError as e:
                mydb.rollback()
                print("Error:", e)
                print("Channels values might be already inserted")

    except psycopg2.Error as e:
        print("Error:", e)
    finally:
        if cursor:
            cursor.close()
        if mydb:
            mydb.close()

# Call the function
channels_table()


import psycopg2

def playlists_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="mirunalini100L",
        database="test1",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS playlists"
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS playlists (
                        PlaylistId varchar(100) PRIMARY KEY,
                        Title varchar(80), 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt timestamp,
                        VideoCount int
                    )'''
    cursor.execute(create_query)
    mydb.commit()

    db = client["youtube_project"]
    coll1 = db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query = '''INSERT INTO playlists (
                            PlaylistId,
                            Title,
                            ChannelId,
                            ChannelName,
                            PublishedAt,
                            VideoCount
                        ) VALUES (%s, %s, %s, %s, %s, %s)'''
        values = (
            row['PlaylistId'],
            row['Title'],
            row['ChannelId'],
            row['ChannelName'],
            row['PublishedAt'],
            row['VideoCount']
        )
        
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("Error:", e)
            mydb.rollback()  # Rollback the transaction to prevent further issues

    # Close the cursor and connection after all operations
    cursor.close()
    mydb.close()

# Call the function to execute the database operations
playlists_table()


import psycopg2

def videos_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="mirunalini100L",
        database="test1",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists videos(
                        Channel_Name varchar(150),
                        Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, 
                        Title varchar(150), 
                        Tags text,
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date timestamp,
                        Duration interval, 
                        Views bigint, 
                        Likes bigint,
                        Comments int,
                        Favorite_Count int, 
                        Definition varchar(10), 
                        Caption_Status varchar(50) 
                    )'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client["youtube_project"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query = '''
            INSERT INTO videos (
                Channel_Name,
                Channel_Id,
                Video_Id, 
                Title, 
                Tags,
                Thumbnail,
                Description, 
                Published_Date,
                Duration, 
                Views, 
                Likes,
                Comments,
                Favorite_Count, 
                Definition, 
                Caption_Status 
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Title'],
            row['Tags'],
            row['Thumbnail'],
            row['Description'],
            row['Published_Date'],
            row['Duration'],
            row['Views'],
            row['Likes'],
            row['Comments'],
            row['Favorite_Count'],
            row['Definition'],
            row['Caption_Status']
        )
                                       
        cursor.execute(insert_query, values)
        mydb.commit()

    # Close the cursor and connection after all operations
    cursor.close()
    mydb.close()

# Call the function to execute the database operations
videos_table()


# comments tables

def comments_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="mirunalini100L",
        database="test1",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS comments"
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS comments(
                           Comment_Id varchar(100) PRIMARY KEY,
                           Video_Id varchar(80),
                           Comment_Text text, 
                           Comment_Author varchar(150),
                           Comment_Published timestamp
                       )'''

    cursor.execute(create_query)
    mydb.commit()

    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        check_query = "SELECT Comment_Id FROM comments WHERE Comment_Id = %s"
        cursor.execute(check_query, (row['Comment_Id'],))
        existing_comment = cursor.fetchone()

        if existing_comment:
            print(f"Comment with ID {row['Comment_Id']} already exists. Skipping insertion.")
        else:
            insert_query = '''INSERT INTO comments (
                                    Comment_Id,
                                    Video_Id,
                                    Comment_Text,
                                    Comment_Author,
                                    Comment_Published
                                )
                                VALUES (%s, %s, %s, %s, %s)
                            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
            cursor.execute(insert_query, values)
            mydb.commit()

    # Close the cursor and connection after all operations
    cursor.close()
    mydb.close()

# Call the function to execute the database operations
comments_table()


def tables_one():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"


def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table

def show_playlists_table():
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        pl_info = pl_data.get("playlist_information", [])
        for playlist in pl_info:
            pl_list.append(playlist)
    playlists_table = pd.DataFrame(pl_list)
    return playlists_table


def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")

    channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = tables_one()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()

#SQL connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="mirunalini100L",
            database= "test1",
            port = "5432"
            )
cursor = mydb.cursor()
    
question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

     
if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
