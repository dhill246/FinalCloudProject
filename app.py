#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.final_project_stack import FinalProjectAnalysisStack


app = cdk.App()
FinalProjectAnalysisStack(app, "FinalProjectStack")

app.synth()
