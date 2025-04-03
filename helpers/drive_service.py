#!/usr/bin/env python3

###########################################################################
#
#  Copyright 2024 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################

"""Google Drive service to process input files from Drive / Sheets."""

from IPython.display import HTML, Markdown, display, clear_output
from google.cloud import storage
import uuid
from googleapiclient.discovery import build
import pandas as pd
import requests
import sys

from configuration import Configuration

def detect_format(url):
    """
    Helper function to detect image format from URL or Google Drive link.
    If no file extension is found in the URL, assumes webp.
    If it's a Google Drive link, extracts the file ID and queries the Drive API for the file type.
    Supported filetypes: https://support.google.com/merchants/answer/7052112

    Args:
        url: URL or Google Drive link of the image.

    Returns:
        Format of the image as a string.
    """
    # Check if it's a Google Drive link
    if 'drive.google.com' in url:
        try:
            # Extract the file ID from the Google Drive link
            file_id = url.split('/')[-2]

            # Build the Drive API client
            drive_service = build('drive', 'v3')

            # Get file metadata
            file_metadata = drive_service.files().get(fileId=file_id, fields='mimeType').execute()
            mime_type = file_metadata.get('mimeType')

            # Extract image format from mime type
            if mime_type.startswith('video/'):
                format = mime_type.split('/')[-1].upper()  # e.g., 'JPEG', 'PNG'
                if format == 'quicktime':
                    format = 'mov'  # Convert 'quicktime' to 'mov'
                return format
            else:
                print(f"Warning: Google Drive file is not an video: {mime_type}")
                return None  # Or handle non-image files as needed

        except Exception as e:
            print(f"Error getting Google Drive file metadata: {e}")
            return None  # Or handle errors as needed

    # If not a Google Drive link, process as a regular URL
    else:
        print(f"Warning: Link provided is not a Google Drive Link: {url}")
        return None

def display_products_table(products):
    """
    Helper function to display product in a legible table

    Args:
        products: A list of dictionaries, where each dictionary represents a product.

    Returns: None

    """
    # Create a DataFrame for display
    df = pd.DataFrame([{
        'Title': p['filename'],
        'Id': p['id'],
        'URL': p['video_url']
    } for p in products])

    # Display as HTML table
    display(HTML(df.to_html(escape=False)))


def upload_blobs_to_gcs(config: Configuration, data_list: list[dict[]], bucket_name: str, destination_folder: str):
    """
    OPTIONAL: Uploads image blobs one-by-one from a list of dictionaries to
    Google Cloud Storage.

    Args:
        config: Configuration class.
        data_list: List of dictionaries, each containing an 'image_blob' field
        bucket_name: Name of the GCS bucket
        destination_folder: Folder path in the bucket (default: 'images/')

    Returns:
        A list of dictionaries with the original data and the public URL added
    """

    # Initialize a GCS client
    client = storage.Client()

    # Get the bucket
    bucket = client.bucket(bucket_name)

    # List to store results
    results = []
    gcs_urls = []
    gcs_drive_mapping = {}

    # Process each dictionary in the list
    for i, data_dict in enumerate(data_list):
        # Check if the image blob exists in the dictionary
        if 'blob' not in data_dict:
            print(f"Warning: Item {i} does not have an 'blob' field. Skipping.")
            results.append(data_dict)  # Add the original dict to results
            continue

        if len(data_dict['filename'].split('.')) == 1:
          # Create a unique filename using UUID
          filename = f"{data_dict['filename']}.{detect_format(data_dict['video_url']).lower()}"
          print(f"Creating filename: {filename}")
        else:
          filename = data_dict['filename']
        destination_blob_name = f"{destination_folder}{filename}"

        # Create a blob object
        blob = bucket.blob(destination_blob_name)

        # Upload the blob
        blob.upload_from_string(
            data_dict['blob'],
            content_type= f"video/{detect_format(data_dict['video_url']).lower()}"  # Adjust content type as needed
        )

        print(f"File {i+1} uploaded to {destination_blob_name}")

        # Add the URL to the dictionary and append to results
        result_dict = data_dict.copy()
        result_dict['gcs_url'] = blob.public_url
        results.append(result_dict)
        gcs_url = f"gs://{bucket_name}/{bucket_path}{filename}"
        gcs_urls.append(gcs_url)
        gcs_drive_mapping[gcs_url] = data_dict['video_url']

    config.set_gcs_drive_mapping(gcs_drive_mapping)
    config.set_videos(gcs_urls)

    print(f"Uploaded {len(results)} files to Google Cloud Storage")
    return results

def download_spreadsheet_data(spreadsheet_id, sheet_name):
    """Downloads data from a Google Sheet, filtering by 'promo?' column.

    Args:
        spreadsheet_id: The ID of the Google Sheet.
        sheet_name: The name of the sheet to read from.

    Returns:
        A pandas DataFrame containing the filtered data, or None if an error occurs.
    """
    try:
        service = build('sheets', 'v4')
        sheet = service.spreadsheets()

        # Fetch all data from the specified sheet
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
        values = result.get('values', [])

        print(values)

        if not values:
            print('No data found.')
            return None

        # Convert data to DataFrame
        df = pd.DataFrame(values[1:], columns=values[0])  # First row as header
        new_columns = {'videoUrl': 'video_url'}
        df.rename(columns=new_columns, inplace=True)

        # Filter rows where 'include?' is True
        if 'include' in df.columns:  # Check if the column exists
            df = df[df['include'].astype(str).str.lower() == 'true']
        else:
            print("Warning: 'include' column not found in the spreadsheet.")

        # Filter rows where 'processed?' is False
        if 'processed' in df.columns:  # Check if the column exists
            df = df[df['processed'].astype(str).str.lower() == 'false']
        else:
            print("Warning: 'processed' column not found in the spreadsheet.")

        # Store all products in an list of dictionaries
        products = []

        for index, row in df.iterrows():
            product = {}
            # Map columns to product attributes.
            product['id'] = index
            product['video_url'] = row.get('video_url', 'No image link found')
            product['filename'] = row.get('filename', 'No filename found')

            # Download the product image
            try:
                # Check if the url is a Google Drive link
                if 'drive.google.com' in product['video_url']:
                    # Extract the file ID from the Google Drive link
                    file_id = product['video_url'].split('/')[-2]
                    # Download the image using the Google Drive API
                    drive_service = build('drive', 'v3')
                    request = drive_service.files().get_media(fileId=file_id)
                    response = request.execute()
                    product['blob'] = response
                else:
                    # Download the image using requests library (for public URLs)
                    response = requests.get(product['video_url'], timeout=10)
                    if response.status_code == 200:
                        product['blob'] = response.content
                    else:
                        product['blob'] = None

                if product['blob']:

                    # Determine image format from the file extension
                    format = detect_format(product['video_url'])

                    # Save in the original format if possible, otherwise use PNG
                    if format:
                        mime_type = f"video/{format.lower()}"
                    else:
                        mime_type = "video/mp4"

            except Exception as e:
                print(f"Error downloading file for {product['filename']}: {str(e)}")
                product['blob'] = None

            products.append(product)
        return products

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
