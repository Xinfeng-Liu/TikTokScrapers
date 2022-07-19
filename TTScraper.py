import requests, json, os
import re as regex
import pandas as pd
from bs4 import BeautifulSoup
import time
import datetime
from datetime import timedelta
from datetime import date
import boto3
from io import StringIO
from dotenv import load_dotenv
load_dotenv()

class TikTokScraper:
    """
    Purpose: This scraper parse the data from TikTok using video url stored in database, and upload the result to S3 bucket.
    Parsing data includes:
        1. Video Engagement Data: 
              like-count
              comment-count
              share-count
        2. Video Creator Data:
              follower-count
              following-count
              heart
              heart-count
              video-count
              digg-count
              author
        3. Vido Data:
              video-id
              text
              create_date
              duration
              format
    """
    def __init__(self):

        # define header
        user_agent = os.getenv("USER_AGENT")
        self.headers = {
            'User-Agent' : user_agent,}
        
        #import video url from database, can be changed to other input formate accordingly
        self.api_url = os.getenv("API_URL")
        self.api_key = os.getenv("API_KEY")
        self.urls = self.api_url + "posts/?key=" + self.api_key
        self.data = requests.get(url=self.urls)
        self.data.encoding = 'utf-8'
        self.data = json.loads(self.data.text)
        self.links = pd.DataFrame.from_dict(self.data)
        self.links.reset_index(inplace=True)
        
        #get html for each url
        self.links["html"] = self.links["url"].apply(self.getHTML)  
    
    def getHTML(self, url):
        """
        Perpose: generate html file based on url
        Input: url
        Output: html
        """
        try:
            data = requests.get(url=url, headers=self.headers)
            time.sleep(5)
            if data.status_code == 404:
                print("Video {} Does Not Exist".format(url))
                data = "DNE"
            elif data.status_code == 200:
                data.encoding = 'utf-8'
                data = data.text
            return data
        except:
            return
        
    def getEngagementInfo(self, html_data):
        """
        Purpose: scrape video engagement features from a html
        Input: html
        Output: a dictionary contains like-count, comment-count, share-count
        """
        engage_info = {}
        if html_data == "DNE":
            engage_info["engage_EM"] = "DNE"
            return engage_info
        try:
            rawVideoMetadata = html_data.split('<script id="SIGI_STATE" type="application/json">')[1].split('</script>')[0]
            videoProps = json.loads(rawVideoMetadata)["ItemModule"]
            video_id = list(videoProps.keys())[0]
            engage_info["video_view_count"] = videoProps[video_id]["stats"]["playCount"]
            engage_info["video_like_count"] = videoProps[video_id]["stats"]["diggCount"]
            engage_info["video_share_count"] = videoProps[video_id]["stats"]["shareCount"]
            engage_info["video_comment_count"] = videoProps[video_id]["stats"]["commentCount"]
            engage_info["engage_EM"] = ""
            return engage_info
        except:
            engage_info["engage_EM"] = "EngageScrapingFailed"
            return engage_info   
    
    def getAuthorInfo(self, html_data):
        """
        Purpose: scrape author features from a html
        Input: html
        Output: a dictionary contains follower-count, following-count, 
                heart-count, video-count, digg-count, author
        """
        author_info = {}
        if html_data == "DNE":
            author_info["creator_EM"] = "DNE"
            return author_info
        try:
            rawVideoMetadata = html_data.split('<script id="SIGI_STATE" type="application/json">')[1].split('</script>')[0]
            videoProps = json.loads(rawVideoMetadata)["ItemModule"]
            video_id = list(videoProps.keys())[0]
            author_info = videoProps[video_id]["authorStats"]
            author_info["creator_username"] = videoProps[video_id]["author"]
            pre = os.getenv("TT_CREATOR_MAIN_PAGE")
            author_info["creator_main_page"] = pre + author_info["creator_username"]
            author_info["creator_EM"] = ""
            return author_info
        except:
            author_info["creator_EM"] = "CreatorScrapingFailed"
            return author_info
        
    def getVideoInfo(self, html_data):
        """
        Purpose: scrape video features from a html
        Input: html
        Output: a dictionary contains video id, text, create-date, format
        """
        video_info = {}
        if html_data == "DNE":
            video_info["video_EM"] = "DNE"
            return video_info
        try:
            rawVideoMetadata = html_data.split('<script id="SIGI_STATE" type="application/json">')[1].split('</script>')[0]
            videoProps = json.loads(rawVideoMetadata)["ItemModule"]
            video_id = list(videoProps.keys())[0]
            video_info["video_id"] = videoProps[video_id]["id"]
            video_info["video_text"] = videoProps[video_id]["desc"]
            unix_timestampe = int(videoProps[video_id]["createTime"])
            date = datetime.datetime.utcfromtimestamp(unix_timestampe).strftime('%Y-%m-%d %H:%M:%S')
            video_info["video_create_date"] = date
            video_info["video_duration"] = videoProps[video_id]["video"]["duration"]
            video_info["video_format"] = videoProps[video_id]["video"]["format"]
            video_info["video_EM"] = ""
            return video_info
        except:
            video_info["video_EM"] = "VideoScrapingFailed"
            return video_info
    
    def getCommentInfo(self, html_data):
        """
        Purpose: scrape comment features from a html
        Input: html
        Output: a dictionary contains comment_creator, comment_text, comment_time
        """
        comment_info_dict = {
            'comment_id':[],
            'comment_owner_username':[],
            'comment_text':[],
            'comment_create_timestamp':[]}
        if html_data == "DNE":
            comment_info_dict["comment_EM"] = "DNE"
            return comment_info_dict
        try:
            rawVideoMetadata = html_data.split('<script id="SIGI_STATE" type="application/json">')[1].split('</script>')[0]
            commentProps = json.loads(rawVideoMetadata)["CommentItem"]
            comment_id = commentProps.keys()
            for cmmt_id in comment_id:
                curr_comment_container = commentProps[cmmt_id]
                comment_info_dict["comment_id"].append(cmmt_id)
                comment_info_dict["comment_owner_username"].append(curr_comment_container["user"])
                comment_info_dict["comment_text"].append(curr_comment_container["text"])
                comment_info_dict["comment_create_timestamp"].append(curr_comment_container["create_time"])
            comment_info_dict["comment_EM"] = ""
            return comment_info_dict
        except:
            comment_info_dict["comment_EM"] = "CommentScrapingFailed"
            return comment_info_dict
        
    def getLinkInBio(self, creator_main_page_url):
        """
        Purpose: link in bio from creator main page
        Input: creator main page url
        Output: return the link if exits, string "LinkInBioDNE" if does not exist
        """
        try:
            user_agent = os.getenv("USER_AGENT")
            cookie = os.getenv("COOKIE")
            headers = {
            'User-Agent':user_agent,
            'Cookie':cookie}
            data = requests.get(url=creator_main_page_url, headers=headers)
            data.encoding = 'utf-8'
            data = data.text
            soup = BeautifulSoup(data, 'html.parser')
            linkinbio_container = soup.find_all(class_="tiktok-847r2g-SpanLink eht0fek2")
            link_in_bio = regex.search('>(.*)</span>', str(linkinbio_container[0])).group(1)
            return link_in_bio
        except:
            return "LinkInBioDNE"
    
    def errorHandling(self, df):
        """
        Purpose: handle the errors 
        Input: dataframe contains error message
        Output: datafram with combined error message and scraper status
        """
        #error handling
        #if comment_count=0, delete the comment scraping error message
        df.loc[df['video_comment_count']==0, 'comment_EM'] = ""
        #combine all error message
        df["Error_Message"] = df[["engage_EM", "creator_EM", "video_EM", "comment_EM"]].apply(lambda x: "/".join(x), axis=1)
        df["Error_Message"] = df["Error_Message"].apply(lambda x: set(x.split("/")))
        #add link in bio error message to the final Error_Message column
        def func(error_message, link_in_bio):
            if link_in_bio == "LinkInBioDNE" and error_message!={"DNE"}:
                error_message.add("LinkInBioScrapingFailed")
            return error_message
        df['Error_Message'] = df.apply(lambda x: func(x['Error_Message'], x['link_in_bio']), axis=1)

        #Scraper status
        df["Status"] = df["Error_Message"].apply(lambda x: "ERROR" if x!={""} else "OK")

        #drop seperate error messages
        df = df.drop(["engage_EM", "creator_EM", "video_EM", "comment_EM"], axis=1)
        return df
    
    def deactiveURL(self, apiURL):
        """
        Purpose: change the DNE url's active status to False in database
        Input: unique url of the video in database
        Output: None
        """
        deactivate_info = {"active": False}
        requests.patch(url=apiURL, json=deactivate_info)
    
    def uploadFile(self, result_dataframe):
        """
        Perpose: upload the result dataframe to s3 bucket in json format
        Input: data frame
        Output: None
        """
        try:
            cur_date_timestamp = int(time.mktime(date.today().timetuple()))
            cur_time_timestamp = int(time.mktime(datetime.datetime.now().timetuple()))
            file_name = (os.getenv("PLATFORM_NAME") + 
                        str(cur_date_timestamp) + 
                        "/" + 
                        str(cur_time_timestamp) + 
                        "_output.json")
            bucket = os.getenv("BUCKET_NAME") # already created on S3
            json_buffer = StringIO()
            result_dataframe.to_json(json_buffer)
            s3_resource = boto3.resource('s3',
                                        endpoint_url=os.getenv("ENDPOINT_URL"),
                                        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                        aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY"))

            s3_resource.Object(bucket, file_name).put(Body=json_buffer.getvalue())
        except Exception as e:
            print("Failed to Upload File to S3 Due to:", str(e))
            return e    
    
    def generateDataFrame(self):
        """
        Purpose: generate a dataframe and csv file that contains all info we need
        Input: export file name
        Output: a datafram and a csv file contians engagement, author, video related features
        """
        try:
            result = self.links["url"].copy()
            info_list = [("engagementInfo", self.getEngagementInfo),
                         ("authorInfo", self.getAuthorInfo),
                         ("VideoInfo", self.getVideoInfo),
                         ("CommentInfo", self.getCommentInfo)]
            for info in info_list:
                self.links[info[0]] = self.links["html"].apply(info[1])
                df_name = "{}_df".format(info[0])
                df_name = pd.json_normalize(self.links[info[0]])
                result = pd.concat([result, df_name], axis=1)
    
            #get link in bio
            result["link_in_bio"] = result["creator_main_page"].apply(self.getLinkInBio)
            
            #get local url for later status change if DNE detected
            result["unique_id"] = self.links["id"]
            result["local_url"] = self.api_url + "posts/" + result["unique_id"] + "?key=" + self.api_key

            #add current date to result
            result["scraper_running_timestamp"] = datetime.datetime.now().isoformat()
            
            #error handling & scraper status
            result = self.errorHandling(result)
            
            #deactive the url if it does not exist
            DNE_URL = result.loc[result["Error_Message"]=={"DNE"}]
            DNE_URL["local_url"].apply(self.deactiveURL)
            
            #upload result to s3
            self.uploadFile(result)
            return result
        except Exception as e:
            return e     


if __name__=="__main__":
    start = time.time()
    scraper = TikTokScraper()
    scraper.generateDataFrame()
    end = time.time()

    # timer
    total_second = end - start
    minute = total_second // 60
    second = round(total_second % 60, 2)
    print("Scraper spent {} minutes {} seconds to extract data.".format(minute, second))



