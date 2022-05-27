import os
import yaml
import json


def read_txt(path):
    with open(path, 'r') as f:
        lines = f.readlines()
    return lines


def read_utf8_text(path):
    with open(path, 'r', encoding='utf8') as f:
        lines = f.readlines()
    return lines


def read_yml(path):
    with open(path, "r") as config:
        data = yaml.load(config, Loader=yaml.FullLoader)
    return data


def read_json(path):
    with open(path, "r") as f:
        data = json.load(f)
    return data


def write_txt(path, data):
    with open(path, 'w') as f:
        f.writelines(data)


def write_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def read_utf8_txt_to_list(path) -> list:
    with open(path) as file:
        lines = file.readlines()
        lines = [line.rstrip() for line in lines]
    return lines
