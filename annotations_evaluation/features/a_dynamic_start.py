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

"""Module to detect Attract: Dynamic Start
Annotations used:
    1. Shot annotations to calculate how dynamic the video is
"""

from annotations_evaluation.annotations_generation import Annotations
from helpers.generic_helpers import load_blob, get_annotation_uri
from configuration import Configuration


def detect_dynamic_start(config: Configuration, feature_name: str, video_uri: str
) -> dict:
    """Detects Dynamic Start
    Args:
        feature_name: the name of the feature
        video_uri: video location in gcs
    Returns:
        dynamic_start: dynamic start evaluation
    """
    annotation_uri = f"{get_annotation_uri(config, video_uri)}{Annotations.GENERIC_ANNOTATIONS.value}.json"
    shot_annotation_results = load_blob(annotation_uri)

    # Feature Dynamic Start
    dynamic_start = False

    # Video API: Evaluate dynamic_start_feature
    if "shot_annotations" in shot_annotation_results:
        first_shot_end_time_off_set = shot_annotation_results.get("shot_annotations")[0]
        seconds = first_shot_end_time_off_set.get("end_time_offset")
        total_ms_first_shot = 0

        if seconds:
            seconds.replace("s", "")
            total_ms_first_shot = (float(seconds) * 1e9) / 1e6

        if total_ms_first_shot < config.dynamic_cutoff_ms:
            dynamic_start = True
    else:
        print(
            f"No Shot annotations found. Skipping {feature_name} evaluation with Video Intelligence API."
        )

    print(f"{feature_name}: {dynamic_start} \n")

    return dynamic_start
