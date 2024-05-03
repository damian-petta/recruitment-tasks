from dataclasses import dataclass
import os


@dataclass
class InformationFromEnv:
    env: str
    input_path: str
    file_extension: str
    output_path: str


def retrieve_environment_variables() -> InformationFromEnv:
    """
    Load config from env.

    Returns:
        InformationFromEnv: An object containing environment-related information.
    """
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Since it's only for the recruitment task and we don't have any GCS/S3 bucket available we need operate on the local storage
    # so, those 4 extra steps are only to make sure we take the data from the proper directory

    input_relative_path = os.getenv("INPUT_RELATIVE_PATH", "../gcs/dev/input/")
    input_path = os.path.normpath(os.path.join(current_directory, input_relative_path))

    output_relative_path = os.getenv("OUTPUT_RELATIVE_PATH", "../gcs/dev/output/")
    output_path = os.path.normpath(
        os.path.join(current_directory, output_relative_path)
    )

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    return InformationFromEnv(
        env=os.getenv("ENVIRONMENT", "DEV").lower(),
        input_path=input_path,
        file_extension="tsv", # I primarily thought that I'll find a better use case for that 
        output_path=output_path,
    )


def get_files_list(input_path: str, file_extension: str) -> list[str]:
    """
    Get list of files in given path. We're skipping files from subdirectories

    Args:
        input_path (str): The path to the directory containing the files.
        file_extension (str): The file extension to filter files.

    Returns:
        list[str]: A list of file paths in the specified directory that match the given file extension.
    """
    files_list = []

    if os.path.isdir(input_path):
        files_in_directory = os.listdir(input_path)
        for file_name in files_in_directory:
            file_path = os.path.join(input_path, file_name)
            if os.path.isfile(file_path):
                if file_name.endswith(file_extension):
                    files_list.append(file_path)
    return files_list


def move_file_to_processed_directory(file_path: str):
    """
    Move file to the processed subdirectory

    Args:
        file_path (str): The path to the file that need to be moved.
    """
    file_name = os.path.basename(file_path)
    processed_folder = os.path.join(os.path.dirname(file_path), "processed")
    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder)
    processed_file_path = os.path.join(processed_folder, file_name)
    os.rename(file_path, processed_file_path)
