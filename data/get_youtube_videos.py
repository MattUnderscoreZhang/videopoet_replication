from googleapiclient.discovery import build
import json


api_key = 'YOUR_API_KEY'
youtube = build('youtube', 'v3', developerKey=api_key)


# Make a request to the API to search for videos based on the 'video_topic' and filter for Creative Commons videos
request = youtube.search().list(
    q='video_topic',
    part='snippet',
    type='video',
    videoLicense='creativeCommon'
)
response = request.execute()

# Extract the URLs and titles of the videos from the JSON response
videos = []
for item in response['items']:
    video = {
        'title': item['snippet']['title'],
        'url': 'https://www
