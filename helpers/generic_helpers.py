#!/usr/bin/env python3

###########################################################################
#
#  Copyright 2024 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################

"""Module to load generic helper functions"""

import json
import os
import urllib
import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas
from moviepy.editor import VideoFileClip
from feature_configs.features import get_feature_configs
from configuration import FFMPEG_BUFFER, FFMPEG_BUFFER_REDUCED, Configuration


def load_blob(annotation_uri: str):
    """Loads a blob to json"""
    # Open and read the JSON file
    with open(annotation_uri, 'r') as file:
        data = json.load(file).get("annotation_results")[0]
    return data

def get_annotation_uri(config: Configuration, video_uri: dict[str, str]) -> str:
    """Helper to translate video to annotation uri."""
    return  config.local_path + "/" + video_uri["filename"] + "/"

def get_reduced_uri(config: Configuration, video_uri: str) -> str:
    """Helper to translate video to reduced video uri."""
    return get_annotation_uri(config, video_uri) + "reduced_1st_5_secs.mp4"

def get_knowledge_graph_entities(config: Configuration, queries: list[str]) -> dict[str, dict]:
    """Get the knowledge Graph Entities for a list of queries
    Args:
        config: All the parameters
        queries: a list of entities to find in KG
    Returns:
        kg_entities: entities found in KG
        Format example: entity id is the key and entity details the value
        kg_entities = {
            "mcy/12": {} TODO (ae) add here
        }
    """
    kg_entities = {}
    try:
        for query in queries:
            service_url = "https://kgsearch.googleapis.com/v1/entities:search"
            params = {
                "query": query,
                "limit": 10,
                "indent": True,
                "key": config.knowledge_graph_api_key,
            }
            url = f"{service_url}?{urllib.parse.urlencode(params)}"
            response = json.loads(urllib.request.urlopen(url).read())
            for element in response["itemListElement"]:
                kg_entity_name = element["result"]["name"]
                # To only add the exact KG entity
                if query.lower() == kg_entity_name.lower():
                    kg_entities[element["result"]["@id"][3:]] = element["result"]
        return kg_entities
    except Exception as ex:
        print(
            f"\n\x1b[31mERROR: There was an error fetching the Knowledge Graph entities. Please check that your API key is correct. ERROR: {ex}\x1b[0m"
        )
        raise

def remove_local_video_files():
    """Removes local video files"""
    if os.path.exists(FFMPEG_BUFFER):
        os.remove(FFMPEG_BUFFER)
    if os.path.exists(FFMPEG_BUFFER_REDUCED):
        os.remove(FFMPEG_BUFFER_REDUCED)

def print_abcd_assessment(brand_name: str, video_assessment: dict) -> None:
    """Print ABCD Assessments"""
 
    print(f"***** ABCD Assessment for brand {brand_name} ***** \n")
    print(f"Asset name: {video_assessment.get('video_blob').get('filename')} \n")

    # Get ABCD evaluations
    if video_assessment.get("annotations_evaluation"):
        print("***** ABCD Assessment using Annotations ***** \n")
        print_score_details(video_assessment.get("annotations_evaluation"))
        
    else:
        print("No annotations_evaluation found. Skipping from priting. \n")

def print_score_details(abcd_eval: dict) -> None:
    """Print score details"""
    total_features = len(abcd_eval.get("evaluated_features"))
    total_features_detected = len(
        [
            feature
            for feature in abcd_eval.get("evaluated_features")
            if feature.get("detected")
        ]
    )
    score = calculate_score(abcd_eval.get("evaluated_features"))
    print(
        f"Video score: {round(score, 2)}%, adherence ({total_features_detected}/{total_features})\n"
    )
    if score >= 80:
        print("Asset result: ✅ Excellent \n")
    elif score >= 65 and score < 80:
        print("Asset result: ⚠ Might Improve \n")
    else:
        print("Asset result: ❌ Needs Review \n")

    print("Evaluated Features: \n")
    for feature in abcd_eval.get("evaluated_features"):
        if feature.get("detected"):
            print(f' * ✅ {feature.get("name")}')
        else:
            print(f' * ❌ {feature.get("name")}')
    print("\n")


def get_call_to_action_api_list() -> list[str]:
    """Gets a list of call to actions

    Returns
        list: call to actions
    """
    return [
        "LEARN MORE",
        "GET QUOTE",
        "APPLY NOW",
        "SIGN UP",
        "CONTACT US",
        "SUBSCRIBE",
        "DOWNLOAD",
        "BOOK NOW",
        "SHOP NOW",
        "BUY NOW",
        "DONATE NOW",
        "ORDER NOW",
        "PLAY NOW",
        "SEE MORE",
        "START NOW",
        "VISIT SITE",
        "WATCH NOW",
    ]


def get_call_to_action_verbs_api_list() -> list[str]:
    """Gets a list of call to action verbs

    Returns
        list: call to action verbs
    """
    return [
        "LEARN",
        "QUOTE",
        "APPLY",
        "SIGN UP",
        "CONTACT",
        "SUBSCRIBE",
        "DOWNLOAD",
        "BOOK",
        "SHOP",
        "BUY",
        "DONATE",
        "ORDER",
        "PLAY",
        "SEE",
        "START",
        "VISIT",
        "WATCH",
    ]


def execute_tasks_in_parallel(tasks: list[any]) -> None:
    """Executes a list of tasks in parallel"""
    results = []
    with ThreadPoolExecutor() as executor:
        running_tasks = [executor.submit(task) for task in tasks]
        for running_task in running_tasks:
            results.append(running_task.result())
    return results


def calculate_score(evaluated_features: list[str]) -> float:
    """Calculate ABCD final score"""
    total_features = len(evaluated_features)
    passed_features_count = 0
    for feature in evaluated_features:
        if feature.get("detected"):
            passed_features_count += 1
    # Get score
    score = (
        ((passed_features_count * 100) / total_features) if total_features > 0 else 0
    )
    return score

def get_feature_by_id(features: list[dict], feature_id: str) -> list[str]:
    """Get feature configs by id"""
    features_found = [
        feature_config
        for feature_config in features
        if feature_config.get("feature_id") == feature_id
    ]
    if len(features_found) > 0:
        return features_found[0]
    return None

def update_annotations_evaluated_features(
    assessment_bq: list[dict], annotations_evaluation: list[dict]
) -> None:
    """Updates default values with annotations evaluated features values
    Finds the feature in assessment_bq and updates with annotations evaluation
    """
    if annotations_evaluation:
        for annotations_eval_feature in annotations_evaluation.get(
            "evaluated_features"
        ):
            feature_found = get_feature_by_id(
                assessment_bq, annotations_eval_feature.get("id")
            )
            if feature_found:
                feature_found["using_annotations"] = True
                feature_found["annotations_evaluation"] = annotations_eval_feature.get(
                    "detected"
                )
            else:
                print(
                    f"Annotations evaluation: Feature {annotations_eval_feature.get('id')} not found. Skipping from storing it in BQ. \n"
                )
    else:
        print("No annotations_evaluation found. Skipping from storing it in BQ. \n")
