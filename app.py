#!/usr/bin/env python3

import aws_cdk as cdk
import os
from cloud_formation.cloud_formation_stack import DecodeCloudFormationStack
from cloud_formation.utils import get_config


env = cdk.Environment(region=get_config()['region_name'], account=os.environ['CDK_DEFAULT_ACCOUNT'])  #TODO: account

app = cdk.App()
DecodeCloudFormationStack(app, "decode-cloud-formation", env=env)

app.synth()
