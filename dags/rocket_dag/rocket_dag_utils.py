from pathlib import Path
from rocket_dag.rocket_dag_consts import AirflowPaths
import requests
import json
import requests.exceptions as exceptions




def create_images_dir(path_to_images_dir: str):
    Path(path_to_images_dir).mkdir(parents=True, exist_ok=True)


def create_json_dir(path_to_json_dir):
    Path(path_to_json_dir).mkdir(parents=True, exist_ok=True)


def save_response_content(path_to_file: str, response: requests.request):
    with open(path_to_file, "wb") as file:
        file.write(response.content)


def fetch_pictures():
    paths = AirflowPaths()
    create_images_dir(paths.images_dir_path)
    if not Path(paths.images_dir_path).is_dir():
        raise ValueError("There is no such dir")
    with open(paths.json_path, 'r') as file:
        launches = json.load(file)
    for item in launches:
        print(item, sep=',')
    print(f"launches {launches}")
    print(f"type {type(launches)}")
    image_urls = [launch['image'] for launch in launches['results']]
    for image_url in image_urls:
        try:
            response = requests.get(image_url)
            image_name  = image_url.split('/')[-1]
            target_file = f"{paths.images_dir_path}/{image_name}"
            save_response_content(target_file, response)
            print(f"Save {image_name} to {target_file}")
        except exceptions.MissingSchema:
            print(f"Invalid URL was detected {image_url}")
        except exceptions.ConnectionError:
            print(f"Could not connect to {image_url}")


def fetch_launches_json():
    paths = AirflowPaths()
    create_json_dir(paths.json_dir)
    URL = 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'
    try:
        response = requests.get(URL)
    except exceptions.MissingSchema:
        print("Invalid launch URL")
    except exceptions.ConnectionError:
        print("Connection failed")
    else:
        with open(paths.json_path, "w", encoding='utf-8') as file:
            json.dump(response.json(), file)
        print(f"json file is dumped to {paths.json_path}")