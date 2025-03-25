# %%
import os
import tempfile
from fastapi import FastAPI, HTTPException
from pytubefix import YouTube
from google.cloud import storage
from io import BytesIO
# %%
# Initialize FastAPI app
app = FastAPI()

# Create a temporary directory for storing the MP4 file
TEMP_DIR = tempfile.gettempdir()

# Initialize Google Cloud Storage client
storage_client = storage.Client(project='data-lake-interno')

def download_video(url: str):
    """
    Downloads the audio stream from the provided YouTube video URL
    and returns the audio file path (MP4).
    """
    try:
        yt = YouTube(url)
        # Try to get Portuguese captions first
        caption = yt.captions.get('a.pt')
        if not caption:
            # If no Portuguese captions, try English
            caption = yt.captions.get('en')
        
        # If no captions in Portuguese or English, return a message
        if not caption:
            return None, "No captions found"
        
        # If captions are found, generate the text
        captions_text = caption.generate_txt_captions()

        # Prepare metadata
        metadata = {
            "title": yt.title,
            "author": yt.author,
            "video_id": yt.video_id,
            "channel_id": yt.channel_id,
            "publish_date": yt.publish_date.strftime("%Y-%m-%d %H:%M:%S"),
            "description": yt.description,
            "keywords": yt.keywords,
            "url": url,
        }

        print(f'Captions downloaded for video: {yt.title}. Metadata: {metadata}')

        return captions_text, metadata

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error downloading video: {str(e)}")

def upload_captions_to_gcs(captions_text: str, metadata: dict, gcs_bucket_name: str):
    """
    Uploads the captions text to Google Cloud Storage as a .txt file with metadata.
    """
    try:
        print(f'Uploading captions to GCS for video. Metadata: {metadata}')
        # Create a temporary .txt file with the captions text
        captions_file_path = os.path.join(TEMP_DIR, f"{metadata.get('video_id')}.txt")
        with open(captions_file_path, "w") as captions_file:
            captions_file.write(captions_text)

        # Upload the .txt file to GCS
        with open(captions_file_path, "rb") as captions_file:
            blob = storage_client.bucket(gcs_bucket_name).blob(f"youtube-captions/{os.path.basename(captions_file_path)}")
            blob.upload_from_file(captions_file, content_type="text/plain")
            
            # Add metadata
            blob.metadata = metadata
            blob.patch()  # Update the metadata on the object
        
        return f"gs://{gcs_bucket_name}/youtube-captions/{os.path.basename(captions_file_path)}"
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error uploading captions to GCS: {str(e)}")
# %%
@app.get('/teste/')
async def download_video(url: str):
    """
    Downloads the audio stream from the provided YouTube video URL
    and returns the audio file path (MP4).
    """
    print(f'Downloading captions for video: {url}')
    list_of_clients = ['WEB', 'WEB_EMBED', 'WEB_MUSIC', 'WEB_CREATOR', 'WEB_SAFARI', 'ANDROID', 'ANDROID_MUSIC', 'ANDROID_CREATOR', 'ANDROID_VR', 'ANDROID_PRODUCER', 'ANDROID_TESTSUITE', 'IOS', 'IOS_MUSIC', 'IOS_CREATOR', 'MWEB', 'TV', 'TV_EMBED', 'MEDIA_CONNECT']
    lista = []
    for client in list_of_clients:
        try:
            yt = YouTube(url, client = 'ANDROID')
            print(f'Client: {client}, yt.captions: {yt.captions}')
            lista.append(f'Client: {client}, yt.captions: {yt.captions}')
        except Exception as e:
            print(f'Error downloading video: {str(e)}')
        # Try to get Portuguese captions first
        caption = yt.captions.get('a.pt')
        if not caption:
            # If no Portuguese captions, try English
            caption = yt.captions.get('en')
        
        # If no captions in Portuguese or English, return a message
        if not caption:
            return None, "No captions found"
        
        # If captions are found, generate the text
        captions_text = caption.generate_txt_captions()

        # Prepare metadata
        metadata = {
            "title": yt.title,
            "author": yt.author,
            "video_id": yt.video_id,
            "channel_id": yt.channel_id,
            "publish_date": yt.publish_date.strftime("%Y-%m-%d %H:%M:%S"),
            "description": yt.description,
            "keywords": yt.keywords,
            "url": url,
        }

        print(f'Captions downloaded for video: {yt.title}. Metadata: {metadata}')

        return lista

@app.post("/convert_and_upload/")
async def convert_and_upload_video(url: str, gs_bucket_name: str):
    """
    Receives a YouTube video URL, retrieves captions, and uploads them as a .txt file to GCS with metadata.
    """
    try:
        # Download captions (in Portuguese or English)
        captions, metadata = download_video(url)

        # print begining of captions and metadata
        print(captions[:100])
        print(metadata)

        if captions == "No captions found":
            return {"message": captions}  # Return the caption error message

        # Upload captions .txt file to GCS
        gcs_url = upload_captions_to_gcs(captions, metadata, gs_bucket_name)

        return {"message": "File uploaded successfully", "gcs_url": gcs_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# %%
