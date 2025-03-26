# %%
import os
import tempfile
from fastapi import FastAPI, HTTPException
from pytubefix import YouTube
from google.cloud import storage
from io import BytesIO
import subprocess
import re
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

def get_video_metadata(url: str):
    """
    .
    """
    try:
        yt = YouTube(url)
        
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

        return metadata

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error downloading video metadata: {str(e)}")

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

def download_subtitles(url, lang='pt-orig', output_dir='tmp'):
    # Build the yt-dlp command as a list of arguments
    command = [
        'yt-dlp', 
        '--skip-download', 
        '--write-auto-subs', 
        '--write-subs', 
        '--sub-lang', lang, 
        '--convert-subs', 'srt', 
        '--sub-format', 'txt', 
        '-o', f"{output_dir}/%(id)s.%(ext)s",  # Set the output template
        url  # The URL of the YouTube video
    ]
    
    # Run the yt-dlp command
    try:
        subprocess.run(command, check=True)
        print(f"Subtitles downloaded successfully to {output_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading subtitles: {e}")

def clean_subtitles(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    cleaned_lines = []
    previous_line = ""
    
    # Regular expression to remove timestamps and line numbers
    timestamp_pattern = re.compile(r'\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}')
    line_number_pattern = re.compile(r'^\d+$')  # Match line numbers

    for line in lines:
        # Remove timestamps using regex
        if timestamp_pattern.match(line.strip()):
            continue
        
        # Remove line numbers (matches lines with only digits)
        if line_number_pattern.match(line.strip()):
            continue
        
        # Remove empty lines
        cleaned_line = line.strip()
        
        # Skip duplicate lines (ignore case sensitivity and extra spaces)
        if cleaned_line and cleaned_line.lower() != previous_line.lower():
            cleaned_lines.append(cleaned_line)
            previous_line = cleaned_line.lower()  # Save in lowercase to handle case-insensitive duplicates

    # Write cleaned lines to a .txt file
    with open(f'{output_file}', 'w', encoding='utf-8') as output:
        output.write("\n".join(cleaned_lines))

# %%
@app.get('/convert_and_upload/')
async def download_video(url: str, gs_bucket_name: str):
    """
    Downloads the video from the provided YouTube URL, extracts the subtitles,
    cleans the subtitles, and uploads the cleaned subtitles to Google Cloud Storage.
    """
    print(f"Downloading video from URL: {url}")

    metadata = get_video_metadata(url)

    print("Metadata retrieved successfully")
    print("Downloading subtitles")

    download_subtitles(url)

    print("Subtitles downloaded successfully")
    print("Cleaning subtitles")

    clean_subtitles(f"tmp/{metadata.get('video_id')}.pt-orig.srt", f"tmp/{metadata.get('video_id')}.txt")

    print("Subtitles cleaned successfully")
    print("Uploading subtitles to GCS")

    # Upload the .txt file to GCS
    with open(f"tmp/{metadata.get('video_id')}.txt", "rb") as captions_file:
        blob = storage_client.bucket(gs_bucket_name).blob(f"""youtube-captions/{os.path.basename(f"{metadata.get('video_id')}.txt")}""")
        blob.upload_from_file(captions_file, content_type="text/plain")
        
        # Add metadata
        blob.metadata = metadata
        blob.patch()

    print("Subtitles uploaded successfully")

    return {"message": f'Dados escritos com sucesso no bucket. Metadata: {metadata}.'}  


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# %%
