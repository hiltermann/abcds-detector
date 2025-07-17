#!/usr/bin/env python3

###########################################################################
#
#    Copyright 2024 Google LLC
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#            https://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
###########################################################################

"""Module that defines global parameters"""

import os

FFMPEG_BUFFER = "reduced/buffer.mp4"
FFMPEG_BUFFER_REDUCED = "reduced/buffer_reduced.mp4"

if not os.path.exists("reduced"):
    os.makedirs("reduced")


class Configuration:
    """Class that stores all parameters used by ABCD."""

    def __init__(self):
        """Initialize with only the required parameters.

          We set all optional parameter defaults in this class because
          we do not want anyone importing the global constants. Hence
          no global variables for hard coded values by design.
        """
        # set parameters
        self.project_id = ""
        self.knowledge_graph_api_key = ""
        self.assessment_file = ""
        self.spreadsheet_id = ""
        self.input_sheet = ""
        self.output_sheet = ""
        self.use_annotations = True

        # set videos
        self.video_blobs = []
        self.video_uris = []

        # set brand
        self.brand_name = ""
        self.brand_variations = []
        self.branded_products = []
        self.branded_products_categories = []
        self.branded_call_to_actions = []

        # set thresholds
        self.early_time_seconds = 5
        self.confidence_threshold = 0.5
        self.face_surface_threshold = 0.15
        self.logo_size_threshold = 3.5
        self.avg_shot_duration_seconds = 2
        self.dynamic_cutoff_ms = 3000

    def set_parameters(self,
        project_id: str,
        knowledge_graph_api_key: str,
        spreadsheet_id: str,
        input_sheet: str,
        output_sheet: str,
        use_annotations: bool,
        local_path: str
    ) -> None:
        """Set the required parameters for ABCD to run.

          Having a separate method for this allows colab multi cell edits.

        Args:
          project_id: Google Cloud Project ID
          knowledge_graph_api_key: Google Cloud API Key (limit this)
          spreadsheet_id: The id of the Google Spreadsheet.
          input_sheet: Input sheet name.
          output_sheet: Output sheet name.
          use_annotations: Use video annotation AI.
          local_path: Local path in colab to store annotation files.
        """
        self.project_id = project_id
        self.knowledge_graph_api_key = knowledge_graph_api_key.strip()
        self.spreadsheet_id = spreadsheet_id
        self.input_sheet = input_sheet
        self.output_sheet = output_sheet
        self.use_annotations = use_annotations
        self.local_path = local_path

    def set_brand_details(self,
        brand_name: str,
        brand_variations: str,
        products: str,
        products_categories: str,
        call_to_actions: str
    ) -> None:
        """Set brand values to help AI evaluate videos.

        Args:
            name: name of brand featured in video.
            variations: comma delimited variations on the brand name.
            products: comma delimited list of products in the video.
            products_categories: comma delimited list of product types.
            call_to_actions: comma delimited list of actions
        """
        self.brand_name = brand_name
        self.brand_variations = [t.strip() for t in brand_variations.split(",")]
        self.branded_products = [t.strip() for t in products.split(",")]
        self.branded_products_categories = [t.strip() for t in products_categories.split(",")]
        self.branded_call_to_actions = [t.strip() for t in call_to_actions.split(",")]

    def set_annotation(self,
        early_time_seconds: int,
        confidence_threshold: float,
        face_surface_threshold: float,
        logo_size_threshold: float,
        avg_shot_duration_seconds: int,
        dynamic_cutoff_ms: int
    ) -> None:
        """Set annotation thresholds to help the AI recognize content.

          Args:
            early_time_seconds: how soon in the video something appears
            confidence_threshold: level of certainty for a positive match
            face_surface_threshold: level of certainty for face detection
            logo_size_threshold: minimal logo size
            avg_shot_duration_seconds: video timing
            dynamic_cutoff_ms: longest clip analyzed
        """
        self.early_time_seconds = early_time_seconds
        self.confidence_threshold = confidence_threshold
        self.face_surface_threshold = face_surface_threshold
        self.logo_size_threshold = logo_size_threshold
        self.avg_shot_duration_seconds = avg_shot_duration_seconds
        self.dynamic_cutoff_ms = dynamic_cutoff_ms
