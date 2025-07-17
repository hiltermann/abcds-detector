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

"""Module to execute the ABCD Detector Assessment"""

import json
import os
import time
from annotations_evaluation.annotations_generation import generate_video_annotations
from annotations_evaluation.evaluation import evaluate_abcd_features_using_annotations
import datetime
from google.colab import sheets
from llms_evaluation.evaluation import evaluate_abcd_features_using_llms
from feature_configs.features import get_feature_configs
import pandas
from prompts.prompts_generator import PromptParams
from helpers.generic_helpers import (
    expand_uris,
    get_blob,
    print_abcd_assessment,
    remove_local_video_files
)
from helpers.vertex_ai_service import LLMParameters
from helpers.bq_service import BigQueryService
from configuration import Configuration
from utils import parse_args, build_abcd_params_config


def execute_abcd_assessment_for_videos(config: Configuration):
  """Execute ABCD Assessment for all brand videos in GCS"""

  for video_blob in config.video_blobs:
    print(f"\n\nProcessing ABCD Assessment for video {video_blob['filename']}... \n")

    video_assessment = { 'video_blob': video_blob }
        
    if config.use_annotations:
      if not os.path.exists(config.local_path):
        os.makedirs(config.local_path)
      generate_video_annotations(config, video_blob, config.local_path)
      annotations_evaluated_features = evaluate_abcd_features_using_annotations(
          config,
          video_blob
      )
      video_assessment["annotations_evaluation"] = {
          "evaluated_features": annotations_evaluated_features,
      }

    print_abcd_assessment(config.brand_name, video_assessment)
        
    if config.output_sheet and config.spreadsheet_id:
      if config.use_annotations:      
        assessment_df = pandas.DataFrame(video_assessment["annotations_evaluation"]["evaluated_features"])
        assessment_df.insert(0, 'Type', "Annotations")

      filename = video_blob["filename"]

      assessment_df.insert(0, 'DriveUrl', video_blob["video_url"])
      assessment_df.insert(0, 'Filename', filename)
      assessment_df.insert(0, 'VideoUrl', "")
      assessment_df.insert(0, 'AnalysisDate', datetime.datetime.now())

    # Remove local version of video files
    remove_local_video_files()

    if config.output_sheet and config.spreadsheet_id:
      sheet = sheets.InteractiveSheet(sheet_id=config.spreadsheet_id, worksheet_name=config.output_sheet, display=False)
      sheet_df = sheet.as_df()
      df_output_sheet = pandas.concat([sheet_df,assessment_df])
      updated_sheet = sheet.update(df_output_sheet)


def main(arg_list: list[str] | None = None) -> None:
  """Main ABCD Assessment execution. See docstring and args.

  Args:
    arg_list: A list of command line arguments

  """

  args = parse_args(arg_list)

  config = build_abcd_params_config(args)

  start_time = time.time()
  print("Starting ABCD assessment... \n")

  if config.video_blobs:
    output = execute_abcd_assessment_for_videos(config)
    print("Finished ABCD assessment. \n")
  else:
    print("There are no videos to process. \n")

  print(f"ABCD assessment took - {(time.time() - start_time) / 60} mins. - \n")


if __name__ == "__main__":
  main()

