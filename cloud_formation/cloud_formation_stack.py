import json
import os
from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_batch_alpha as batch,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_ecr_assets as ecr_assets,
)
from .utils import get_config


class CloudFormationStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = get_config()

        # QUEUES
        config_sqs = config['sqs']
        queue_names = config_sqs['queue_names']
        queues = []
        for queue_name in queue_names:
            queue_name = queue_name + '.fifo' if not queue_name.endswith('.fifo') else queue_name
            queue = sqs.Queue(
                self, queue_name, queue_name=queue_name, fifo=True, content_based_deduplication=True,
                visibility_timeout=Duration.minutes(config_sqs['visibility_timeout'])
            )
            queues.append(queue)

        # DISPATCHER
        config_dispatcher = config['dispatcher']
        # Role
        dispatcher_role = iam.Role(
            self, config_dispatcher['lambda_role_id'], assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(policy_name)
                for policy_name in (
                    'SecretsManagerReadWrite', 'AmazonSQSReadOnlyAccess',
                    'AmazonElasticFileSystemReadOnlyAccess', 'AWSBatchFullAccess',
                )
            ]
        )
        # Function
        dispatcher_function = lambda_.Function(
            self, config_dispatcher['lambda_fun_id'], function_name=config_dispatcher['lambda_fun_name'],
            runtime=eval(f"lambda_.Runtime.{config_dispatcher['runtime']}"), role=dispatcher_role, handler='lambda_script.lambda_handler',
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), 'dispatcher_code')), timeout=Duration.minutes(1),  #TODO: timeout
            environment={'Variables': json.dumps({'config_json': get_config()})},
        )
        # Scheduler
        dispatcher_scheduler = events.Rule(
            self, config_dispatcher['rule_id'], rule_name=config_dispatcher['rule_name'],
            schedule=events.Schedule.rate(Duration.minutes(config_dispatcher['rate'])),
            enabled=True, targets=[targets.LambdaFunction(handler=dispatcher_function)]
        )
        #TODO: might need to add permission in lambda, SQS event source

        # VPC
        config_vpc = config['vpc']
        vpc = ec2.Vpc(self, config_vpc['vpc_name'])

        # ECR
        config_ecr = config['ecr']
        repo = ecr.Repository(
            self, config_ecr['repo_id'], repository_name=config_ecr['repo_name']
        )
        #TODO: temp (for testing)
        dockerimage = ecr_assets.DockerImageAsset(
            self, config_ecr['dockerimage_id'], directory=os.path.dirname(__file__),
        )

        # BATCH
        config_batch = config['batch']
        # Environment
        batch_compute_env = batch.ComputeEnvironment(
            self, config_batch['compute_env_id'], compute_environment_name=config_batch['compute_env_name'],
            compute_resources=batch.ComputeResources(
                type=batch.ComputeResourceType.SPOT,
                vpc=vpc, vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_NAT),
                #TODO: PRIVATE_WITH_NAT?
                minv_cpus=config_batch['minv_cpus'], maxv_cpus=config_batch['maxv_cpus'],
                instance_types=[ec2.InstanceType(i_t) for i_t in config_batch['instance_types']]
            )
        )
        # Queue
        batch_queue = batch.JobQueue(
            self, config_batch['queue_id'], job_queue_name=config_batch['queue_name'],
            compute_environments=[batch.JobQueueComputeEnvironment(compute_environment=batch_compute_env, order=1)],
        )
        # Job definition
        batch_job_def = batch.JobDefinition(
            self, config_batch['job_def_id'], job_definition_name=config_batch['job_def_name'],
            container=batch.JobDefinitionContainer(image=ecs.ContainerImage.from_docker_image_asset(dockerimage)),
        )
