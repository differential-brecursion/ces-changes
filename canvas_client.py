"""
This module provides functionality to interact with the Canvas platform.
It includes functionalities like fetching user ID, managing folders,
uploading files, and checking storage quota.
"""

import logging
import os
import requests
from canvasapi import Canvas
from configuration_settings import TOTAL_QUOTA_MB, logger


class CanvasInteraction:
    """Handles interactions with the Canvas platform."""

    def __init__(self, base_url, api_token):
        self.base_url = base_url
        self.api_token = api_token
        self.headers = {'Authorization': f'Bearer {api_token}'}
        self.canvas = Canvas(base_url, api_token)

    def get_user_id(self, username):
        """ Fetches the user ID based on the provided username."""
        try:
            #Construct the URL to fetch the user data
            url = f'{self.base_url}/api/v1/accounts/self/users?search_term={username}'

            # Make a GET request to the constructed URL
            response = requests.get(url, headers=self.headers)

            if not 200 <= response.status_code < 300:  # If it's not a successful response
                if response.status_code == 404:  # Specific case for 404 error
                    logger.error("User ID %s associated with a file but user does not exist in Canvas", username)
                    return None
                else:
                    logger.error(
                        "Failed to fetch user ID for username %s. Response Code: %s. Response Content: %s",
                        username, response.status_code, response.content
                    )
                    return None

            user_data = response.json()
            if not user_data or not isinstance(user_data, list):
                logger.error(
                    "Unexpected response format while fetching user ID for username %s.", username)
                return None

            logger.debug("Users matched for username %s: %s", username, user_data)
            return user_data[0]['id']

        except requests.RequestException as error:
            logger.error("Error while fetching user ID for username %s. Error: %s", username, error)
            return None

    def get_or_create_folder(self, user_id, folder_path, folder_id=None):
        """
        If folder_id is provided, returns the folder object for that ID.
        Otherwise, retrieve or create a folder in Canvas based on the user ID and specified folder path.
        """
        try:
            # If a folder_id is provided, retrieve and return the folder directly
            if folder_id:
                return self.canvas.get_folder(folder_id)

            # If no folder_id is provided, start the process to retrieve or create the folder
            user = self.canvas.get_user(user_id)
            folders = folder_path.split('/')
            current_folder_id = None

            for folder_name in folders:
                # Get the list of folders based on the current folder context
                if current_folder_id:
                    folders_list = self.canvas.get_folder(current_folder_id).get_folders(as_user_id=user_id)
                else:
                    folders_list = user.get_folders(as_user_id=user_id)

                # Check if the folder already exists
                found_folder = next((folder for folder in folders_list if folder.name == folder_name), None)

                # If the folder doesn't exist, create it
                if not found_folder:
                    if current_folder_id:
                        found_folder = self.canvas.get_folder(current_folder_id).create_folder(
                            name=folder_name, as_user_id=user_id
                        )
                    else:
                        found_folder = user.create_folder(name=folder_name, as_user_id=user_id)

                current_folder_id = found_folder.id

            return current_folder_id

        except Exception as e:
            # Logging the error can be helpful for debugging purposes
            logger.error(f"Error while retrieving or creating folder {folder_path} for user {user_id}. Error: {str(e)}")
            return None


    def upload_file(self, folder_id, file_name, file_path, content_type, user_id):
        """
        Initiate and complete the file upload process to a specified folder in Canvas.
        """

        try:
            # Check if the specified file exists
            if not os.path.exists(file_path):
                logger.error(f"File {file_path} does not exist. Leaving upload.")
                return

            logger.debug(f"Checking if file {file_name} exists in folder {folder_id}...")
            logger.info(f"Attempting to upload {file_name} to folder {folder_id}...")

            # Construct the URL for the folder where the file will be uploaded
            folder_url = f'{self.base_url}/api/v1/folders/{folder_id}/files'

            # Create the payload for the initial upload request
            payload = {
                'name': file_name,
                'size': os.path.getsize(file_path),
                'content_type': content_type,
            }
            if user_id:
                payload['as_user_id'] = user_id #masquerade as the user

            # Initiate the file upload
            response = requests.post(folder_url, data=payload, headers=self.headers)
            if not (200 <= response.status_code < 300):
                logger.error(f"Failed to initiate upload for {file_name}. Response: {response.content}")
                return response.status_code

            # Extract the upload URL and parameters from the response
            upload_data = response.json()
            upload_url = upload_data['upload_url']
            upload_params = upload_data['upload_params']

            # Complete the file upload
            with open(file_path, 'rb') as file:
                files = {'file': (file_name, file)}
                response = requests.post(upload_url, data=upload_params, files=files)

            if not (200 <= response.status_code < 300):
                logger.error(f"Failed to upload {file_name}. Response: {response.content}")

            return response.status_code

        except Exception as e:
            logger.error(f"Error while uploading {file_name} to folder {folder_id}. Error: {str(e)}")
            return None


class QuotaManager:
    """Manages and checks storage quota for a Canvas user."""

    quota_logger = logging.getLogger('QuotaCheck')

    def __init__(self, user_id, canvas_api):
        self.user_id = user_id
        self.canvas_api = canvas_api
        self.total_quota_mb = float(TOTAL_QUOTA_MB)


    def get_remaining_space(self):
        """Check and return the amount of storage space remaining for the user in megabytes."""
        url = f'{self.canvas_api.base_url}/api/v1/users/{self.user_id}/files/quota'
        headers = self.canvas_api.headers
        response = requests.get(url, headers=headers)
        if 200 <= response.status_code < 300:
            try:
                quota_bytes = response.json()["quota"]
                used_storage_bytes = response.json()["quota_used"]
                total_quota_mb = quota_bytes / (1024 * 1024)
                used_storage_mb = used_storage_bytes / (1024 * 1024)
                remaining_storage_mb = total_quota_mb - used_storage_mb
                return remaining_storage_mb
            except KeyError as e:
                self.quota_logger.error(f"Error parsing quota information for user ID {self.user_id}. Missing key: {e}")
                return None  # Return None if there's a KeyError
        else:
            self.quota_logger.error(
                f"Failed to retrieve quota information for user ID {self.user_id}. Status code: {response.status_code}, Response: {response.text}")
            return None  # Return None if the status code isn't in the 200s range


if __name__ == "__main__":
    pass
