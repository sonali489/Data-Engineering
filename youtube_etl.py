import os
import pandas as pd
import googleapiclient.discovery




def run_etl():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "AIzaSyDM67sdraKOHUlsvHyWDw8Jgg-Tl2j7Qa0"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)

    request = youtube.commentThreads().list(
        part="snippet, replies",
        videoId="9KdtRXSaMQA"
    )
    response= request.execute()
    
    
    
    comments = []
    for item in response.get('items', []):
        snippet = item.get('snippet', {})
        topLevelComment = snippet.get('topLevelComment', {})
        snippet_comment = topLevelComment.get('snippet', {})
        
        author = snippet_comment.get('authorDisplayName', "")
        comment_text = snippet_comment.get('textOriginal', "")
        publish_time = snippet_comment.get('publishedAt', "")
        
        comment_info = {'author': author, 'comment': comment_text, 'published_at': publish_time}
        comments.append(comment_info)

    print(f'Finished processing {len(comments)} comments.')
    
    df = pd.DataFrame(comments)
    df.to_csv("comments.csv", index=False) 


    

