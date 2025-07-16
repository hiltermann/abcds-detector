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

"""Module to generate video annotations using the Video Intelligence API"""

import json

from enum import Enum
from google.cloud import videointelligence
from google.cloud.videointelligence import VideoContext
from google.cloud import videointelligence_v1 as videointelligence2
from google.protobuf.json_format import MessageToDict # Import for protobuf to dict conversion
from configuration import Configuration
from helpers.generic_helpers import (
    get_blob,
    execute_tasks_in_parallel,
)


class Annotations(Enum):
    """Annotation types enum"""

    GENERIC_ANNOTATIONS = "generic_annotations"
    FACE_ANNOTATIONS = "face_annotations"
    PEOPLE_ANNOTATIONS = "people_annotations"
    SPEECH_ANNOTATIONS = "speech_annotations"


def standard_annotations_detection(
    video_client: videointelligence.VideoIntelligenceServiceClient,
    video_blob: str,
    local_path: str
) -> None:
    """Detect the following standard annotations: Text, Shot, Logo and Label"""
    features = [
        videointelligence.Feature.TEXT_DETECTION,
        videointelligence.Feature.SHOT_CHANGE_DETECTION,
        videointelligence.Feature.LOGO_RECOGNITION,
        videointelligence.Feature.LABEL_DETECTION,
    ]
    operation = video_client.annotate_video(
        request={
            "features": features,
            "input_content": video_blob
        }
    )
    print(f"\nProcessing video for {str(features)} annotations...")
    response = operation.result(timeout=800)

    # Convert the protobuf response object to a Python dictionary
    response_data = MessageToDict(response)

    # Write the dictionary to a JSON file
    with open(local_path, 'w') as f:
        json.dump(response_data, f, indent=2) # Use indent for pretty-printing

    print(
        f"\nFinished processing video for {str(features)} annotations...\n"
    )


def custom_annotations_detection(
    video_client: videointelligence.VideoIntelligenceServiceClient,
    context: VideoContext,
    features: list[videointelligence.Feature],
    video_blob: str,
    local_path: str
) -> None:
    """Detect the following custom annotations: Face, People and Speech"""

    operation = video_client.annotate_video(
        request={
            "features": features,
            "input_content": video_blob,
            "video_context": context
        }
    )
    print(f"\nProcessing video for {str(features)} annotations...")
    response = operation.result(timeout=800)

    # Convert the protobuf response object to a Python dictionary
    response_data = MessageToDict(response)

    # Write the dictionary to a JSON file
    with open(local_path, 'w') as f:
        json.dump(response_data, f, indent=2) # Use indent for pretty-printing

    print(
        f"\nFinished processing video for {str(features)} annotations...\n"
    )


def generate_video_annotations(config: Configuration, video_blob: str,
                               local_path: str) -> None:
    """Generates video annotations for videos in Google Cloud Storage"""

    standard_video_client = videointelligence.VideoIntelligenceServiceClient()
    custom_video_client = videointelligence2.VideoIntelligenceServiceClient()

    # Face Detection
    face_config = videointelligence.FaceDetectionConfig(
        include_bounding_boxes=True, include_attributes=True
    )
    face_context = videointelligence.VideoContext(face_detection_config=face_config)

    # People Detection
    person_config = videointelligence2.types.PersonDetectionConfig(
        include_bounding_boxes=True,
        include_attributes=True,
        include_pose_landmarks=True,
    )
    person_context = videointelligence2.types.VideoContext(
        person_detection_config=person_config
    )

    # Speech Detection
    speech_config = videointelligence.SpeechTranscriptionConfig(
        language_code="en-US", enable_automatic_punctuation=True
    )
    speech_context = videointelligence.VideoContext(
        speech_transcription_config=speech_config
    )

    # Video annotations processing

    tasks = []
    # annotation_path = get_annotation_uri(config, video_uri)

    standard_annotations_path = (
        f"{local_path}/{Annotations.GENERIC_ANNOTATIONS.value}.json"
    )
    face_annotations_path = f"{local_path}/{Annotations.FACE_ANNOTATIONS.value}.json"
    people_annotations_path = (
        f"{local_path}/{Annotations.PEOPLE_ANNOTATIONS.value}.json"
    )
    speech_annotations_path = (
        f"{local_path}/{Annotations.SPEECH_ANNOTATIONS.value}.json"
    )

    # Detect Standard annotations & Custom annotations
   
    tasks.append(
        lambda: standard_annotations_detection(
            standard_video_client, video_blob, standard_annotations_path
        ),
    )
    tasks.append(
        lambda: custom_annotations_detection(
            standard_video_client,
            face_context,
            [videointelligence.Feature.FACE_DETECTION],
            video_blob,
            face_annotations_path
        )
    )
    tasks.append(
        lambda: custom_annotations_detection(
            custom_video_client,
            person_context,
            [videointelligence2.Feature.PERSON_DETECTION],
            video_blob,
            people_annotations_path,
        )
    )
    tasks.append(
        lambda: custom_annotations_detection(
            standard_video_client,
            speech_context,
            [videointelligence.Feature.SPEECH_TRANSCRIPTION],
            video_blob,
            speech_annotations_path,
        )
    )

    # Execute annotations generation tasks only for the ones that haven't been processed.
    execute_tasks_in_parallel(tasks)
