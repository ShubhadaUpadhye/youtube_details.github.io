from flask import Flask ,request,jsonify,render_template,redirect,url_for,send_file
import mysql.connector
from googleapiclient.discovery import build
import pymongo
from urllib.request import  urlopen as urReq
import html
#import gc
import logging as log
#from flask_pymongo import PyMongo

app=Flask(__name__,template_folder='templates')
#app.config["MONGO_URI"] = "mongodb://localhost:27017/mydatabase"
#mongo = PyMongo(app)

log.basicConfig(filename='record.log',level=log.INFO,format='%(levelname)s %(asctime)s %(name)s %(message)s')

video_id_list = []
name_list_mongo = []
comment_list_mongo = []
title_list_1 = []
video_url_list=[]
chan_ids=[]
#youtube,api key
api_key = 'AIzaSyB7og-sgYVOcVbnnqZ4xpWhibsSR3F82gQ'
youtube = build('youtube', 'v3', developerKey=api_key)

##connecting ,ysql workbench
print("connecting to mysql workbench")
mydb =mysql.connector.connect(host='localhost', user='root', passwd='mysqlp@$$', database='youtube_details')
#postgres://youtube_data_user:XYxCBt67G8gIjo4JoV06u9Q5IWhOKmzs@dpg-ch1r105gk4qarqm4ss90-a.oregon-postgres.render.com/youtube_data
mycursor = mydb.cursor(buffered=True)
app.logger.info(mydb.is_connected())
try:
    mycursor.execute("create database if not exists youtube_details")
    mycursor.execute(
        "create table if not exists youtube_channel(sl_no int,channel_name varchar(45),channel_id varchar(45),channel_tot_videos int,channel_subscribers int,tot_view_count int)")
    mycursor.execute(
        "create table if not exists youtube_video(sl_no int,video_id varchar(20),title_of_video varchar(100),video_link varchar(100),video_thumbnail varchar(100),video_views int,video_likes int,total_comments int)")
except Exception as e:
    app.logger.exception(e)
else:
    app.logger.info("database created")
    app.logger.info("channel table created")
    app.logger.info("videos table created")

##connecting mongodb
app.logger.info("Connecting to mongodb")
client = pymongo.MongoClient("mongodb+srv://shubhadadu:datascience@shuhadaupadhye.l98sn.mongodb.net/?retryWrites=true&w=majority")
db = client['youtube_scraped_data']
my_coll=db['names_and_comments']
my_coll.delete_many({})
my_coll_thumbs=db['thumbnails']
my_coll_thumbs.delete_many({})
channel_coll=db['channel_id_list']
try:
    records = my_coll.find()
except Exception as e:
   app.logger.exception(e)
else:
    records_list = list(records)

#Connecting front end
@app.route("/",methods=['GET','POST'])
def home():
    if request.method == "GET":
        return render_template("index.html")

@app.route("/channel",methods=['POST','GET'])
def channel():
    print("fetching ChannelName by POST method")
    if request.method=='POST':
        Channel_Name = request.form.get('Channel_Name')
        print(Channel_Name)
        data = channel_coll.find()
        print("validating ChannelName")
        for d in data:
            for k, v in d.items():
                if Channel_Name.isupper():
                    Channel_Name=Channel_Name.lower()
                    if k==Channel_Name:
                        chan_ids.append(v)
                        app.logger.info(chan_ids)
                        break
                elif k==Channel_Name:
                        chan_ids.append(v)
                        app.logger.info(chan_ids)
                        break

            else:
                if k!=Channel_Name:
                    app.logger.info("incorrect channel name")
                    return render_template("index.html",error="Invalid channel name")

##accessing youtube channel data
    print("extracting youtube details starting with channel details with input as channel_id")
    def channel_data(youtube,channel_id):
        for ids in channel_id:
            try:
                request = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=ids)
                response = request.execute()
            except Exception as e:
                app.logger.exception(e)
            else:
                return response
    channel_result = channel_data(youtube,channel_id=chan_ids)
    channel_details = channel_result['items'][0]
    channel_title = channel_details['snippet']['localized']['title']
    channel_id_1 = channel_details['id']
    playlist_id = channel_details['contentDetails']['relatedPlaylists']['uploads']
    channel_view_count = int(channel_details['statistics']['viewCount'])
    channel_subscriber_count = int(channel_details['statistics']['subscriberCount'])
    channel_video_count = int(channel_details['statistics']['videoCount'])

    ##fetching top 50 video_ids
    print("fetching top 50 videos video_id's")
    def video_ids():
        try:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50)
            response = request.execute()
        except Exception as e:
            app.logger.exception(e)
        else:
            return response

    video_id_result = video_ids()
    for n in range(len(video_id_result['items'])):
        try:
            videoid = video_id_result['items'][n]['contentDetails']['videoId']
        except Exception as e:
            app.logger.exception(e)
        else:
            video_id_list.append(videoid)

    ##truncating mysql tables
    print("truncating sql tables")
    try:
        mycursor.execute("truncate youtube_channel")
        mycursor.execute("truncate youtube_video")
    except Exception as e:
        app.logger.exception(e)
    else:
        print("channel table truncated")
        print("videos table truncated")

    ##inserting channel data in mysql table
    print("inserting channel details in mysql_table")
    sl_no = 0
    try:
        channel_data_inserting = (sl_no, channel_title, channel_id_1, channel_video_count, channel_subscriber_count,channel_view_count)
        channel_data_uploading = "insert into youtube_channel(sl_no,channel_name,channel_id,channel_tot_videos,channel_subscribers,tot_view_count) values(%s,%s,%s,%s,%s,%s)"
        mycursor.execute(channel_data_uploading, channel_data_inserting)
        mydb.commit()
    except Exception as e:
        app.logger.exception(e)
    else:
        print("channel data inserted into mysql table")
    print("fetching video details")
    for video_id in video_id_list:
        sl_no = sl_no + 1
        def video_list(video_id):
            try:
                request = youtube.videos().list(
                    part="snippet,contentDetails,statistics", id=video_id)
                response = request.execute()
            except Exception as e:
                app.logger.exception(e)
            else:
                return response
        video_result = video_list( video_id)
        video_details = video_result['items'][0]
        video_ids=video_details['id']
        video_title = video_details['snippet']['title']
        for characters in video_title:
            if characters == '.':
                video_title = video_title.replace(characters, '-')
        title_list_1.append(video_title)
        video_thumbnail = video_details['snippet']['thumbnails']['medium']['url']
        video_no_of_views = int(video_details['statistics']['viewCount'])
        video_no_of_likes = int(video_details['statistics']['likeCount'])
        video_no_of_comments = int(video_details['statistics']['commentCount'])

        app.logger.info("fetching video_url")
        def video_url(video_id):
            try:
                url = "https://youtu.be/" + video_id
                response = urReq(url)
            except Exception as e:
                app.logger.exception(e)
            else:
                return (url)
        website = video_url(video_id)
        video_url_list.append(website)
        app.logger.info("inserting video details in mysql_table")
        try:
            videos_data_inserting = (sl_no,video_ids, video_title, website, video_thumbnail, video_no_of_views, video_no_of_likes, video_no_of_comments)
            videos_data_uploading = "insert into youtube_video(sl_no,video_id,title_of_video,video_link,video_thumbnail,video_views,video_likes,total_comments) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            mycursor.execute(videos_data_uploading, videos_data_inserting)
            mydb.commit()
        except Exception as e:
            app.logger.exception(e)
        else:
            app.logger.info("videos data inserted into mysql table")

        #upadting data in mongodb
        '''try:
            t = Thumbnail(website)
            image = t.fetch(size='default')
        except Exception as e:
            print(e)
        else:
            print('fetching image to upload')
        # creating dictionary
        thumbs_dict = {video_title:image}
        try:
            my_coll_thumbs.insert_one(thumbs_dict)
        except Exception as e:
            print(e)
        else:
            print("image inserted")'''
        app.logger.info("fetching comments commentors on videos")
        def comment_thread(video_id):
            try:
                request = youtube.commentThreads().list(part="replies,snippet", videoId=video_id, maxResults=100)
                response = request.execute()
            except Exception as e:
                app.logger.exception(e)
            else:
                return response
        comment_result = comment_thread(video_id)
        comment_details = comment_result['items']
        for i in range(len(comment_details)):
            try:
                commentor_name_mongo = comment_details[i]['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comment_mongo = comment_details[i]['snippet']['topLevelComment']['snippet']['textOriginal']
                try:
                    comment_mongo = html.unescape(comment_mongo)
                except Exception as e:
                    app.logger.exception(e)
                else:
                    name_list_mongo.append(commentor_name_mongo)
                    comment_list_mongo.append(comment_mongo)
            except Exception as e:
                app.logger.exception(e)
            else:
                mongo_dict = {'sl_no':sl_no,'title':video_title,'names': name_list_mongo, 'comments': comment_list_mongo}
        else:
            if len(comment_details) == 0:
                try:
                    commentor_name_mongo = "No commentors or comments disabled"
                    comment_mongo = "No comments on video or comments disabled"
                    name_list_mongo.append(commentor_name_mongo)
                    comment_list_mongo.append(comment_mongo)
                except Exception as e:
                    print(e)
                else:
                    mongo_dict = {'sl_no': sl_no, 'title': video_title, 'names': name_list_mongo,
                                  'comments': comment_list_mongo}
        app.logger.info("inserting data into Mongodb")
        try:
            my_coll.insert_one(mongo_dict)
        except Exception as e:
            app.logger.exception(e)
        else:
            app.logger.info("data inserted in mongodb")
        name_list_mongo.clear()
        comment_list_mongo.clear()
        #mongo_dict.clear()
    #fetching channel data from mysql
    app.logger.info("fetching channel_data from mysql_table")
    try:
        mycursor.execute('use youtube_details')
        mycursor.execute('select * from  youtube_channel')
        chan_stats= mycursor.fetchall()
    except Exception as e:
        app.logger.exception(e)
    else:
        app.logger.info('channel_sql_data fetched')
    return render_template('channel.html',chan_sql_data=chan_stats)

@app.route("/video",methods=['GET'])
def result():
    app.logger.info("fetching videos data from mysql_table")
    try:
        mycursor.execute("use youtube_details")
        mycursor.execute("select * from youtube_video")
    except Exception as e:
        app.logger.exception(e)
    else:
        video_data=mycursor.fetchall()
        return render_template("video.html",data=video_data)

@app.route("/comments",methods=['GET'])
def comments():
    app.logger.info("fetching names and comments from Mongodb using GET method")
    data=[]
    try:
        for documents in my_coll.find():
            data.append(documents)
    except Exception as e:
       app.logger.exception(e)
    else:
        return render_template("comments.html",data=data)

@app.route('/download_images',methods=['GET'])
def download_images():
    app.logger.info("fetching thumbnails_url from mysql_table using GET method")
    try:
        mycursor.execute("select title_of_video,video_thumbnail from youtube_video")
    except Exception as e:
        app.logger.exception(e)
    else:
        data=mycursor.fetchall()
    return render_template("downloads.html",data=data)


if __name__=='__main__':
    app.run(debug=True)






