# Task 1

## DEscription 
This folder contains my proposal solution for Task 1, which is part of the recruitment process for the Junior Data Engineer position at Tidio. Task 1 mainly involves parsing the URLs given in a .tsv file and transferring them to another .tsv file, which contains all information from each URL in a separate column.

## What can be improved?
If I had some more time, I would probably add:
- A basic implementation of CI/CD with GitHub Actions, mainly for executing all tests defined in the `./tests` directory and for code formatting.
- More unit tests.
- A `pyproject.toml` config file.



## How to run this?

- linux

```
python3 -m venv ./.venv
source ./.venv/bin/activate
pip3 install -r requirements.txt
python3 ./app/main.py

```

- docker

```
docker build -t task1-script .
docker run task1-script
```