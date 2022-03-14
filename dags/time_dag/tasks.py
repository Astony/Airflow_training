from pathlib import Path
import pandas as pd


def create_folder(path):
    print(f"Create folder with path {path}")
    Path(path).mkdir(parents=True, exist_ok=True)


def create_folders(**context):
    input_path = context['templates_dict']['input_folder']
    output_path = context['templates_dict']['output_folder']
    create_folder(input_path)
    create_folder(output_path)


def get_stats_csv(**context):
    input_path = context['templates_dict']['input_path']
    output_path = context['templates_dict']['output_path']
    # output_folder = '/'.join(output_path.split('/')[:-1])
    print(f"Read json {input_path}")
    events = pd.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index()
    print(f"Save csv {output_path}")
    stats.to_csv(output_path)






