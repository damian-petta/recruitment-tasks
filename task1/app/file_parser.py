import os
from urllib.parse import urlparse, parse_qs
import pandas as pd
from storage import InformationFromEnv, move_file_to_processed_directory
import logging

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


def map_url_params_to_columns(url: str) -> dict:
    """
    Parses the query parameters of a URL and maps them to specific columns based on a predefined schema.

    Args:
        url (str): The URL string containing the query parameters.

    Returns:
        dict: A dictionary that match a schema defined in SCHEMA_MAPPING variable
              The original URL is included with the key "url".
    """
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    mapped_columns = {}
    mapped_columns["url"] = url
    for param, column in SCHEMA_MAPPING.items():
        if param in query_params:
            mapped_columns[column] = query_params[param][0]

    return mapped_columns


def parse_file(file_path: str, env_info: InformationFromEnv):
    """
    Parse a file, extract URL parameters, map them to columns based on a predefined schema, and save the results.

    Args:
        file_path (str): The path to the file to parse.
        env_info (InformationFromEnv): An object containing environment-related information.
    """

    file_name = os.path.basename(file_path)
    output = os.path.join(env_info.output_path, file_name)
    if os.path.exists(output):
        os.remove(output)
    with pd.read_csv(file_path, delimiter="\t", chunksize=CHUNK_SIZE) as reader:
        records = []
        for chunk in reader:
            chunk["mapped_columns"] = chunk["url"].apply(map_url_params_to_columns)
            records += chunk["mapped_columns"].tolist()

        df = pd.DataFrame(records, columns=["url"] + list(SCHEMA_MAPPING.values()))
        df.to_csv(
            output, index=False, sep="\t", mode="a", header=not os.path.exists(output)
        )
        logging.info(f"Processed file is present in the {output} directory")
    if env_info.env == "prod":
        move_file_to_processed_directory(file_path)
