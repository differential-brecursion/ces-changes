"""
This module is responsible for handling the processing of files for multiple usernames. Main Script.
"""

# Standard library imports
import os
import re
import shutil
import logging
import csv
import configparser

from configuration_settings import *

config = configparser.ConfigParser()
config.read('config.ini')

from canvas_client import CanvasInteraction, QuotaManager

# Set up logging
logger = logging.getLogger(__name__)

# Mapping file extensions to content types
content_types = {
    '.pdf': 'application/pdf'
}


def extract_unique_usernames_from_files(semester_folder):
    """Processes 'semester_folder', extracts/cleans usernames, and returns a list of unique usernames."""
    logger.info("[extract_unique_usernames_from_files] Starting to extract unique usernames from files...")

    all_entries = os.listdir(semester_folder)
    # Filter out directories
    all_files = [f for f in all_entries if os.path.isfile(os.path.join(semester_folder, f))]

    total_files_uploader = len(all_files)
    logger.info(
        f"[extract_unique_usernames_from_files] Total files in semester directory at start: {total_files_uploader}")

    usernames_set = set()

    for source_file in all_files:
        # Check if file matches expected pattern, if not continue to next file
        if not re.match(r".*_[a-zA-Z0-9()-]+\.\w+$", source_file):
            continue

        try:
            match = re.search(r"_([a-zA-Z0-9()-]+)\.\w+$", source_file)
            if not match:
                logger.error(
                    f"[extract_unique_usernames_from_files] Failed to extract username from filename: {source_file}")
                continue

            username = match.group(1)
            usernames_set.add(username)  # Adding to set
        except Exception as e:
            logger.error(
                f"[extract_unique_usernames_from_files] Error processing filename {source_file}. Reason: {str(e)}")
            continue

    return list(usernames_set)

def separate_users_based_on_quota(usernames_set, canvas_client):
    """Separates users into two lists based on their quota usage."""

    users_within_quota = []
    users_exceeding_quota = []

    for user_name in usernames_set:
        try:
            user_id = canvas_client.get_user_id(user_name)
            if not user_id:
                #logger.error(f"[seperate_users_based_on_quota] User ID not found for username {user_name}")
                continue

            
        except Exception as e:
            logger.error(f"[separate_users_based_on_quota] Unexpected error processing username {user_name}. Reason: {str(e)}")
            continue

    return users_within_quota, users_exceeding_quota

def upload_user_files_to_canvas_based_on_quota(semester_folder, canvas_client, usernames):
    """Upload user files to Canvas based on the user's available quota."""
    try:
        # Process users within quota and upload their files
        for user_name in usernames:
            user_id = canvas_client.get_user_id(user_name)

            folder_path = EVAL_REPORTS_PATH
            sub_folder_path = USER_SEMESTER_FOLDER_PATH
            sub_folder_id = canvas_client.get_or_create_folder(user_id, f"{folder_path}/{sub_folder_path}")

            if not sub_folder_id:
                logger.error(f"Could not get or create a subfolder for user ID {user_id}")
                continue

            # Generate a list of tuples containing file paths and their sizes
            # tuple has the format: (file_path, file_size_in_MB)
            user_files_with_sizes = [
                (os.path.join(semester_folder, file),
                 os.path.getsize(os.path.join(semester_folder, file)) / (1024 * 1024))
                for file in os.listdir(semester_folder) if re.search(rf"{user_name}.pdf", file)
            ]

            for file, size in user_files_with_sizes:
                # Extract the file name from the full path of the file.
                file_name = os.path.basename(file)

                # Determine the file's extension (e.g., .pdf, .txt) and then find the corresponding MIME content type.
                # If the file extension is not found in the 'content_types' dictionary (should be pdf),
                # default to 'application/octet-stream', general binary file MIME type.
                content_type = content_types.get(os.path.splitext(file)[-1], 'application/octet-stream')

                # Attempt to upload the file to Canvas. The method returns a response code.
                response_code = canvas_client.upload_file(sub_folder_id, file_name, file, content_type, user_id)

                # Check the response code. If it's outside the range, the upload was not successful.
                if not (200 <= response_code < 300):
                    logger.error(f"Failed to upload {file_name} for user {user_name}")
                    # check user remaining space
                    try:
                        quota_manager = QuotaManager(user_id, canvas_client)
                        remaining_space_mb = quota_manager.get_remaining_space()

                        if remaining_space_mb is None or remaining_space_mb < 0:
                            logger.error(f"Unable to retrieve quota information for user {user_name}")
                        specific_path = EXCEEDED_STORAGE_DIR_PATH
                        exceeded_storage_dir = os.path.join(specific_path, f'file_storage_exceeded_{user_name}')
                        os.makedirs(exceeded_storage_dir, exist_ok=True)
                        dest_path = os.path.join(exceeded_storage_dir, os.path.basename(file))
                        shutil.move(file, dest_path)
                        logger.info(f"Moved file {file} to {dest_path}")
                    except Exception as e:
                        logger.error(f"Unable to retrieve quota information for user {user_name}")
                        continue
            
    except Exception as e:
        logger.error(f"Error processing operations. Reason: {str(e)}")


def process_all_files(semester_folder, userfile, canvas_client):
    """Main processing function to handle all files for a given semester."""
    try:
        semester_name = os.path.basename(semester_folder)
        logger.info(f"[process_all_files] Running this semester {semester_name}")

        with open(userfile, mode='r', encoding='utf-8-sig') as csvfile:
            csvreader = csv.DictReader(csvfile)
            user_data = {row['login_id'].lower(): row['user_id'] for row in csvreader}

        # Always extract usernames from files
        usernames = extract_unique_usernames_from_files(semester_folder)

        # Get list of users based on quota
        upload_user_files_to_canvas_based_on_quota(semester_folder, canvas_client, usernames)
        
        logger.info("[process_all_files] Finished processing all files.")

    except (FileNotFoundError, csv.Error) as e:
        logger.error(f"[process_all_files] Error with the CSV file {userfile}: {str(e)}")
        raise e


if __name__ == "__main__":

    canvas_url = config['CANVAS_API']['url']
    api_token = config['CANVAS_API']['token']
    canvas_client = CanvasInteraction(canvas_url, api_token)

    # Ensure to get the absolute path for the semester folder
    config_directory = os.path.dirname(os.path.abspath('config.ini'))
    semester_folder = os.path.join(config_directory, config['DIRECTORY_PATHS']['semester_directory_path'])

    userfile = config['DIRECTORY_PATHS']['user_files']

    process_all_files(semester_folder, userfile, canvas_client)

    for handler in logger.handlers:
        handler.close()




