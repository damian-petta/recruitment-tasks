from dataclasses import dataclass
import pandas as pd
import os
import logging
from urllib.parse import urlparse, parse_qs

logging.basicConfig(level=logging.INFO)


SCHEMA_MAPPING = {
    "a_bucket": "ad_bucket",
    "a_type": "ad_type",
    "a_source": "ad_source",
    "a_v": "schema_version",
    "a_g_campaignid": "ad_campaign_id",
    "a_g_keyword": "ad_keyword",
    "a_g_adgroupid": "ad_adgroup_id",
    "a_g_creative": "ad_creative",
}


CHUNK_SIZE = 10**4


@dataclass
class InformationFromEnv:
    env: str
    input_path: str
    file_extension: str
    output_path: str


def retrieve_environment_variables() -> InformationFromEnv:
    """
    Load config from env.
    """

    return InformationFromEnv(
        env=os.getenv("ENVIRONMENT", "DEV").lower(),
        input_path=os.getenv("INPUT_PATH", "./gcs/dev/input/"),
        file_extension="tsv",
        output_path=os.getenv("OUTPUT_PATH", "./gcs/dev/output/"),
    )


def get_files_list(input_path: str, file_extension: str) -> list[str]:
    """
    Get list of files in given path. We're skipping files from subdirectories
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


def map_url_params_to_columns(url: str) -> dict:
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    mapped_columns = {}
    mapped_columns["url"] = url
    for param, column in SCHEMA_MAPPING.items():
        if param in query_params:
            mapped_columns[column] = query_params[param][0]

    return mapped_columns


def parse_file(file_path: str, env_info: InformationFromEnv):
    file_name = os.path.basename(file_path)
    output = os.path.join(env_info.output_path, file_name)
    if os.path.exists(output):
        os.remove(output)
    with pd.read_csv(
        file_path, delimiter="\t", chunksize=CHUNK_SIZE
    ) as reader:
        records = []
        for chunk in reader:
            chunk["mapped_columns"] = chunk["url"].apply(map_url_params_to_columns)
            records += chunk["mapped_columns"].tolist()
            
        df = pd.DataFrame(records, columns = ["url"] + list(SCHEMA_MAPPING.values()))
        df.to_csv(
            output, index=False, sep="\t", mode="a", header=not os.path.exists(output)
        )


def main():
    envinfo = retrieve_environment_variables()

    files_to_parse = get_files_list(envinfo.input_path, envinfo.file_extension)

    errors = []

    for file in files_to_parse:
        try:
            parse_file(file, envinfo)
        except Exception as e:
            errors.append(f"\n FILE: {file} \n ERROR MESSAGE: {e}")

    if len(errors) > 0:
        for error in errors:
            logging.error(error)

        raise Exception("Script has not completed successfully")


if __name__ == "__main__":
    main()
