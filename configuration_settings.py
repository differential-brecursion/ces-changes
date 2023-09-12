"""
Configuration Settings Module

This module handles the configuration settings for interacting with the Canvas platform.
"""

import os
import time
import zipfile
import logging
import configparser
import pytz
import boto3


# Define the log directory and ensure it exists
log_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_directory, 'process_files.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Start of Configuration Loading ---
config = configparser.ConfigParser()
config.read('config.ini')

# Configuration values
canvas_url = config['CANVAS_API']['URL']
api_token = config['CANVAS_API']['TOKEN']
headers = {"Authorization": f"Bearer {api_token}"}

USER_FILE = config['DIRECTORY_PATHS']['USER_FILES']

config_directory = os.path.dirname(os.path.abspath('config.ini'))
SEMESTER_DIRECTORY_PATH = os.path.join(config_directory, config.get('DIRECTORY_PATHS', 'semester_directory_path'))

USER_FILES = config['DIRECTORY_PATHS']['USER_FILES']
EXCEEDED_STORAGE_DIR_PATH = config['DIRECTORY_PATHS']['EXCEEDED_STORAGE_DIR_PATH']
EVAL_REPORTS_PATH = config['DIRECTORY_PATHS']['EVAL_REPORTS_PATH']
USER_SEMESTER_FOLDER_PATH = config['DIRECTORY_PATHS']['USER_SEMESTER_FOLDER_PATH']
TOTAL_QUOTA_MB = config['DIRECTORY_PATHS']['TOTAL_QUOTA_MB']

# Ensure all required configurations are present
required_configs = {
    'DIRECTORY_PATHS': ['SEMESTER_DIRECTORY_PATH', 'USER_FILES', 'EXCEEDED_STORAGE_DIR_PATH', 'EVAL_REPORTS_PATH', 'USER_SEMESTER_FOLDER_PATH'],
    'CANVAS_API': ['URL', 'TOKEN']
}


def setup_directories():
    """Set up the necessary directories."""
    # Setup exceeded storage directory
    exceeded_storage_dir_path = config.get('DIRECTORY_PATHS', 'exceeded_storage_dir_path')

    logger.debug("Config directory: %s", config_directory)
    # Check if the directory path is empty or None, and if so, use the DIRECTORY_PATHS
    if not exceeded_storage_dir_path:
        exceeded_storage_dir_path = os.path.join(config_directory, 'file_storage_exceeded')

    if not os.path.exists(exceeded_storage_dir_path):
        os.makedirs(exceeded_storage_dir_path)
        config['DIRECTORY_PATHS']['exceeded_storage_dir_path'] = 'file_storage_exceeded'  # Storing only the folder name

        exceeded_storage_file_path = os.path.join(exceeded_storage_dir_path, 'exceeded_storage')
        with open(exceeded_storage_file_path, 'w', encoding="utf-8") as file:
            file.write('')

    # Setup semester directory
    semester_folder_name = config.get('DIRECTORY_PATHS', 'semester_directory_path', fallback=None)
    if not semester_folder_name:  # If there's no specific semester_directory_path set
        semester_folder_name = "semester_directory_path"
    semester_directory = os.path.join(config_directory, semester_folder_name)

    if not os.path.exists(semester_directory):
        os.makedirs(semester_directory)
        config['DIRECTORY_PATHS']['semester_directory_path'] = "semester_directory_path"
        update_config()

    logger.info("Semester Directory: %s", semester_directory)

    return semester_directory



def download_and_extract_semester_from_zip():
    """Download the .zip file from the specified S3 bucket and extract semester name."""
    try:
        s3 = boto3.client('s3')
        bucket_name = 'vt-tlos-ec2-intake'
        prefix = 'i-0646aca9bcf24dacc/'

        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # Filter out to get the .zip files
        zip_files = [obj for obj in response.get('Contents', []) if obj['Key'].endswith('.zip')]

        if not zip_files:
            logger.error("No .zip files found in bucket '%s' with prefix '%s'.", bucket_name, prefix)
            return None, None

        # there will be only one .zip file so taking the first
        zip_file = zip_files[0]
        download_path = os.path.join(SEMESTER_DIRECTORY_PATH, zip_file['Key'].split('/')[-1])
        s3.download_file(bucket_name, zip_file['Key'], download_path)
        logger.info("Downloaded the .zip file to %s", download_path)

        # Extract the semester name from the filename
        filename = zip_file['Key'].split('/')[-1]
        parts = filename.split('_')
        semester_name = "_".join(parts[1:-1]) if len(parts) >= 3 else None

        return download_path, semester_name

    except Exception as e:
        logger.error("Error while downloading from S3 and extracting semester name: %s", str(e))
        return None, None

def handle_zip_files(semester_directory):
    """
    Downloads the zip file from the S3 bucket, then extracts it into the given directory.

    """
    # Download the ZIP file from S3
    zip_filepath, semester_name = download_and_extract_semester_from_zip()
    if not zip_filepath:
        logger.error("Failed to download ZIP from S3.")
        return None

    # Check if the downloaded ZIP file exists
    if not os.path.exists(zip_filepath):
        logger.error("The file %s does not exist.", zip_filepath)
        return None

    # Check if the provided path is indeed a ZIP file
    if not zipfile.is_zipfile(zip_filepath):
        logger.error("The file %s is not a valid ZIP file.", zip_filepath)
        return None

    # Extract the file name from the path
    zip_filename = os.path.basename(zip_filepath)

    # Extract the ZIP file
    try:
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            logger.info("Files in the zip: %s", zip_ref.namelist())  # Log names of files inside the zip
            zip_ref.extractall(semester_directory)
        logger.info("Successfully extracted %s to %s", zip_filename, semester_directory)

        # Return the name of the unzipped folder as a relative path
        return os.path.relpath(semester_directory, start=config_directory)
    except Exception as e:
        logger.error("Error extracting %s. Reason: %s", zip_filename, str(e))
        return None

def update_config():
    """Update the configuration file."""
    with open('config.ini', 'w', encoding="utf-8") as configfile:
        config.write(configfile)


def get_most_recent_file(bucket_name, prefix):
    """Get the most recent file from the S3 bucket based on the provided prefix."""
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' not in response:
        logger.error("No objects found in bucket '%s' with prefix '%s'.", bucket_name, prefix)
        return None

    most_recent_object = max(response['Contents'], key=lambda x: x['LastModified'])

    # Download the file
    download_path = os.path.join(config_directory, most_recent_object['Key'].split('/')[-1])
    s3.download_file(bucket_name, most_recent_object['Key'], download_path)
    logger.info("Downloaded the most recent file to %s", download_path)

    return most_recent_object

def main():
    """Main method to execute the script operations."""
    semester_directory = setup_directories()  # Capture only the semester_directory value and ignore the uploader_files_directory

    # Get the unzipped folder's relative path and update the config
    unzipped_folder_relative_path = handle_zip_files(semester_directory)
    if unzipped_folder_relative_path:
        config['DIRECTORY_PATHS']['semester_directory_path'] = unzipped_folder_relative_path

    bucket_name = 'vt-tlos-user-integration'
    prefix = 'prod/canvas/'
    most_recent_file = get_most_recent_file(bucket_name, prefix)
    download_path, semester_name = download_and_extract_semester_from_zip()
    if download_path and semester_name:
        logger.info("Downloaded ZIP file %s containing semester data for %s", download_path, semester_name)
        handle_zip_files(download_path)
    else:
        logger.error("Failed to download or extract semester data from ZIP.")

    if most_recent_file:
        last_modified = most_recent_file['LastModified'].astimezone(pytz.timezone(time.tzname[0]))
        logger.info("Most recent file: %s", most_recent_file['Key'])
        logger.info("Size: %.1f MB", most_recent_file['Size'] / 1e6)
        logger.info("Last Modified: %s (%s)", last_modified.strftime('%B %d, %Y, %H:%M:%S'), time.tzname[0])

        # Update the user_files entry in the config
        most_recent_file_name = most_recent_file['Key'].split('/')[-1]
        config['DIRECTORY_PATHS']['user_files'] = most_recent_file_name
    else:
        logger.info("No recent files found.")

    update_config()


if __name__ == '__main__':
    main()

