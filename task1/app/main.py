from file_parser import parse_file
from storage import retrieve_environment_variables, get_files_list
import logging

logging.basicConfig(level=logging.INFO)


def main():
    envinfo = retrieve_environment_variables()

    files_to_parse = get_files_list(envinfo.input_path, envinfo.file_extension)

    errors = []

    for file in files_to_parse:
        try:
            parse_file(file, envinfo)
        except Exception as e:
            # In case one file in folder be corrupted we still can parse others and show errors at the end of process
            errors.append(f"\n FILE: {file} \n ERROR MESSAGE: {e}")

    if len(errors) > 0:
        for error in errors:
            logging.error(error)

        raise Exception("Script has not completed successfully")


if __name__ == "__main__":
    main()
