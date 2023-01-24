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
    aws_ecr_assets as ecr_assets,
)
from .utils import get_config


class DecodeCloudFormationStack(Stack):
    """Decode Cloud Formation Stack.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = get_config()
        # queues
        queues = self.get_queues(config)
        # API
        # api_fargate = self.get_api(config)
        # dispatcher
        dispatcher_function, dispatcher_scheduler = self.get_scheduled_dispatcher(config)
        # batch
        batch_compute_env, batch_queue, batch_job_def = self.get_batch_resources(config)
        # postprocessor
        # postprocessor_function = self.get_postprocessor(config)

    def get_scheduled_dispatcher(self, config: dict):
        """Sets up the dispatcher and a regular scheduler.
        """
        config_dispatcher = config['dispatcher']
        # Role
        dispatcher_role = iam.Role(
            self, config_dispatcher['lambda_role_id'], assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(policy_name)
                              for policy_name in ('AmazonSQSFullAccess', 'AWSBatchFullAccess')],
        )
        # Function
        # create dockerimage for lambda
        scripts_path = os.path.join(os.path.dirname(__file__), 'dispatcher_code')
        with open(os.path.join(scripts_path, 'Dockerfile'), 'w') as df:
            df.writelines(line + '\n' for line in [  # why is this function even called writeLINES???
                f"FROM public.ecr.aws/lambda/python:{config_dispatcher['python_version']}",
                "COPY ./requirements.txt .",
                'RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"',
                "COPY . ${LAMBDA_TASK_ROOT}",
                "ADD utils utils",
                'CMD [ "lambda_script.lambda_handler" ]',
            ])
        # create lambda function
        dispatcher_function = lambda_.DockerImageFunction(
            self, config_dispatcher['lambda_fun_id'], function_name=config_dispatcher['lambda_fun_name'],
            role=dispatcher_role, timeout=Duration.minutes(2),
            code=lambda_.DockerImageCode.from_image_asset(scripts_path, cmd=["lambda_script.lambda_handler"]),
            environment={'config_json': json.dumps(get_config())},
        )
        # Scheduler
        dispatcher_scheduler = events.Rule(
            self, config_dispatcher['rule_id'], rule_name=config_dispatcher['rule_name'],
            schedule=events.Schedule.rate(Duration.minutes(config_dispatcher['rate'])), enabled=True,
        )
        # attach scheduler
        dispatcher_scheduler.add_target(targets.LambdaFunction(dispatcher_function))
        return dispatcher_function, dispatcher_scheduler

    def get_queues(self, config: dict):
        """Sets up the SQS queues.
        """
        config_sqs = config['sqs']
        queue_names = config_sqs['queue_names'].values()
        queues = []
        for queue_name in queue_names:
            # using fifo queues
            queue_name = queue_name + '.fifo' if not queue_name.endswith('.fifo') else queue_name
            queue = sqs.Queue(
                self, queue_name, queue_name=queue_name, fifo=True, content_based_deduplication=True,
                visibility_timeout=Duration.seconds(config_sqs['visibility_timeout'])
            )
            queues.append(queue)
        return queues

    def get_batch_resources(self, config: dict):
        """Sets up everything that is required for the batch jobs:
        Compute environment, job queue, job definition, VPC, Docker container.
        """
        # Vpc
        config_vpc = config['vpc']
        vpc = ec2.Vpc(
            self, config_vpc['vpc_name'],
            #TODO: config
            subnet_configuration=[{'cidrMask': 24, 'name': 'private', 'subnetType': ec2.SubnetType.PRIVATE_ISOLATED}],
            gateway_endpoints={
                'S3': ec2.GatewayVpcEndpointOptions(service=ec2.GatewayVpcEndpointAwsService.S3),
            },  # endpoint since no NAT (else: nat_gateways=1, nat_gateway_provider=ec2.NatProvider.instance(instance_type=ec2.InstanceType('t2.micro'))))
        )
        # interfaces to other services (required since private and no NAT)
        vpc.add_interface_endpoint('EC2', service=ec2.InterfaceVpcEndpointAwsService.EC2)
        vpc.add_interface_endpoint('ECR', service=ec2.InterfaceVpcEndpointAwsService.ECR)
        vpc.add_interface_endpoint('ECS', service=ec2.InterfaceVpcEndpointAwsService.ECS)
        # Docker image
        config_batch = config['batch']
        #TODO: temp (for testing): after probably GitHub actions to tag&push to ECR + version handling via overriding
        dockerimage = ecr_assets.DockerImageAsset(
            self, config_batch['dockerimage_id'], directory=os.path.dirname(__file__),
        )
        # Efs
        #TODO: EFS
        # Compute environment
        batch_compute_env = batch.ComputeEnvironment(
            self, config_batch['compute_env_id'], compute_environment_name=config_batch['compute_env_name'],
            #TODO: config
            compute_resources=batch.ComputeResources(
                type=batch.ComputeResourceType.ON_DEMAND,
                vpc=vpc, vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
                minv_cpus=config_batch['minv_cpus'], maxv_cpus=config_batch['maxv_cpus'],
                instance_types=[ec2.InstanceType(i_t) for i_t in config_batch['instance_types']]
            )
        )
        # Job queue
        batch_queue = batch.JobQueue(
            self, config_batch['queue_id'], job_queue_name=config_batch['queue_name'],
            compute_environments=[batch.JobQueueComputeEnvironment(compute_environment=batch_compute_env, order=0)],
        )
        # Execution role
        batch_role = iam.Role(
            self, config_batch['batch_role_id'], assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(policy_name)
                for policy_name in (
                    'AmazonS3FullAccess', 'CloudWatchFullAccess',
                )
            ]
        )
        # Job definition
        batch_job_def = batch.JobDefinition(
            self, config_batch['job_def_id'], job_definition_name=config_batch['job_def_name'],
            #TODO: config, dockerimage on ECR
            container=batch.JobDefinitionContainer(
                image=ecs.ContainerImage.from_docker_image_asset(dockerimage),
                execution_role=batch_role
            ),
            timeout=Duration.seconds(18000),
        )
        return batch_compute_env, batch_queue, batch_job_def

    def get_api(self, config: dict):
        pass

    def get_postprocessor(self, config: dict):
        pass
