"""From template, still not adapted.
"""
import aws_cdk as core
import aws_cdk.assertions as assertions
from cloud_formation.cloud_formation_stack import CloudFormationStack


def _test_sqs_queue_created():
    app = core.App()
    stack = CloudFormationStack(app, "cloud-formation")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::SQS::Queue", {
        "VisibilityTimeout": 300
    })


def _test_sns_topic_created():
    app = core.App()
    stack = CloudFormationStack(app, "cloud-formation")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::SNS::Topic", 1)
