#!/usr/bin/env python3

import aws_cdk as cdk

from cloud_formation.cloud_formation_stack import CloudFormationStack


#TODO: allow flexibility
env_EU = cdk.Environment(region='eu-central-1')

app = cdk.App()
CloudFormationStack(app, "decode-cloud-formation", env=env_EU)

app.synth()
