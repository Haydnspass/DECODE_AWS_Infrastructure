import json
import os
from constructs import Construct
from aws_cdk import (
    Duration,
    RemovalPolicy,
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
    aws_efs as efs,
    aws_s3 as s3,
)
from .utils import get_config


class DecodeCloudFormationStack(Stack):
    """Decode Cloud Formation Stack.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = get_config()
        # Queues
        queues = self.get_queues(config)
        # VPC
        vpc = self.get_vpc(config)
        # Dispatcher
        dispatcher_function, dispatcher_scheduler = self.get_scheduled_dispatcher(config)
        # File system
        file_system = self.get_file_system(config, vpc=vpc)
        # Batch
        batch_compute_env, batch_queue, batch_job_def = self.get_batch_resources(config, vpc=vpc, file_system=file_system)
        # S3
        bucket = self.get_bucket(config)
        # postprocessor
        # postprocessor_function = self.get_postprocessor(config)
        # API
        # api_fargate = self.get_api(config)

    def get_scheduled_dispatcher(self, config: dict):
        """Sets up the dispatcher and a regular scheduler.
        """
        config_dispatcher = config['dispatcher']
        # Role
        dispatcher_role = iam.Role(
            self, 'decode_lambda_role_id', assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
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
            self, 'decode_lambda_fun_id', function_name=config_dispatcher['lambda_fun_name'],
            role=dispatcher_role, timeout=Duration.minutes(2),
            code=lambda_.DockerImageCode.from_image_asset(scripts_path, cmd=["lambda_script.lambda_handler"]),
            environment={'config_json': json.dumps(get_config())},
        )
        # Scheduler
        dispatcher_scheduler = None
        #dispatcher_scheduler = events.Rule(
        #    self, 'decode_rule_id', rule_name=config_dispatcher['rule_name'],
        #    schedule=events.Schedule.rate(Duration.minutes(config_dispatcher['rate'])), enabled=True,
        #)
        #dispatcher_scheduler.add_target(targets.LambdaFunction(dispatcher_function))
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

    def get_vpc(self, config: dict):
        """Creates isolated, private VPC and creates required gateways/interface endpoints.
        """
        config_vpc = config['vpc']
        vpc = ec2.Vpc(
            self, config_vpc['vpc_name'],
            subnet_configuration=[{'cidrMask': 24, 'name': 'private', 'subnetType': ec2.SubnetType.PRIVATE_ISOLATED}],
            gateway_endpoints={
                'S3': ec2.GatewayVpcEndpointOptions(service=ec2.GatewayVpcEndpointAwsService.S3),
            },  # endpoint since no NAT
        )
        # interfaces to other services (required since private and no NAT)
        vpc.add_interface_endpoint('EC2', service=ec2.InterfaceVpcEndpointAwsService.EC2)
        vpc.add_interface_endpoint('ECR_docker', service=ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER)
        vpc.add_interface_endpoint('ECR', service=ec2.InterfaceVpcEndpointAwsService.ECR)
        vpc.add_interface_endpoint('ECS_agent', service=ec2.InterfaceVpcEndpointAwsService.ECS_AGENT)
        vpc.add_interface_endpoint('ECS_telemetry', service=ec2.InterfaceVpcEndpointAwsService.ECS_TELEMETRY)
        vpc.add_interface_endpoint('ECS', service=ec2.InterfaceVpcEndpointAwsService.ECS)
        vpc.add_interface_endpoint('EFS', service=ec2.InterfaceVpcEndpointAwsService.ELASTIC_FILESYSTEM)
        vpc.add_interface_endpoint('CloudWatch_logs', service=ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS)
        return vpc

    def get_file_system(self, config: dict, vpc: ec2.Vpc):
        """Gets the EFS filesystem for batch.
        """
        config_efs = config['efs']
        file_system = efs.FileSystem(
            self, 'decode_file_system_id', file_system_name=config_efs['file_system_name'],
            vpc=vpc,
            lifecycle_policy=efs.LifecyclePolicy.AFTER_14_DAYS,  #TODO: config
            performance_mode=efs.PerformanceMode.GENERAL_PURPOSE,  #TODO: config
            out_of_infrequent_access_policy=efs.OutOfInfrequentAccessPolicy.AFTER_1_ACCESS,  #TODO: config
            removal_policy=RemovalPolicy.DESTROY,
        )
        return file_system

    def get_batch_resources(self, config: dict, vpc: ec2.Vpc, file_system: efs.FileSystem):
        """Sets up everything that is required for the batch jobs:
        Compute environment, job queue, job definition, Docker container.
        """
        config_batch = config['batch']
        # Docker image
        #TODO: temp (for testing): after probably GitHub actions to tag&push to ECR + version handling via overriding
        dockerimage = ecr_assets.DockerImageAsset(
            self, 'decode_dockerimage_id', directory=os.path.join(os.path.dirname(__file__), 'batch_code'),
        )
        # Compute environment
        batch_compute_env = batch.ComputeEnvironment(
            self, 'decode_compute_env_id2', compute_environment_name=config_batch['compute_env_name'],
            #TODO: config
            compute_resources=batch.ComputeResources(
                type=batch.ComputeResourceType.ON_DEMAND,
                vpc=vpc, vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
                minv_cpus=config_batch['minv_cpus'], maxv_cpus=config_batch['maxv_cpus'],
                instance_types=[ec2.InstanceType(i_t) for i_t in config_batch['instance_types']]
            )
        )
        file_system.connections.allow_default_port_from(batch_compute_env)
        # Job queue
        batch_queue = batch.JobQueue(
            self, 'decode_queue_id', job_queue_name=config_batch['queue_name'],
            compute_environments=[batch.JobQueueComputeEnvironment(compute_environment=batch_compute_env, order=0)],
        )
        # Execution role
        batch_role = iam.Role(
            self, 'decode_batch_role_id', assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(policy_name)
                for policy_name in (
                    'AmazonS3FullAccess', 'CloudWatchFullAccess', 'AmazonEC2ContainerRegistryReadOnly',
                    'AmazonElasticFileSystemFullAccess',  #TODO: fine-tune
                )
            ]
        )
        # Job definition
        #TODO: will need multiple (one per decode version), since apparently we can't override the container per-job
        batch_job_def = batch.JobDefinition(
            self, 'decode_job_def_id', job_definition_name=config_batch['job_def_name'],
            #TODO: config, dockerimage on ECR
            container=batch.JobDefinitionContainer(
                image=ecs.ContainerImage.from_docker_image_asset(dockerimage),
                execution_role=batch_role,
                job_role=batch_role,  #TODO:
                mount_points=[ecs.MountPoint(container_path='/files', read_only=False, source_volume='efs_volume')],
                volumes=[ecs.Volume(
                    name='efs_volume',  #TODO: additional config?
                    efs_volume_configuration=ecs.EfsVolumeConfiguration(file_system_id=file_system.file_system_id)
                )],
                gpu_count=config_batch['job_def_gpu_count'],
                memory_limit_mib=config_batch['job_def_memory'],
                vcpus=config_batch['job_def_vcpus'],
                environment={'NVIDIA_REQUIRE_CUDA': 'cuda>=11.0', 'NVIDIA_DRIVER_CAPABILITIES': 'all'},  # required to use GPUs
            ),
            timeout=Duration.seconds(18000), #TODO: memory
        )
        return batch_compute_env, batch_queue, batch_job_def

    def get_bucket(self, config: dict):
        config_s3 = config['s3']
        logs_lifecycle_rule = s3.LifecycleRule(
            id='decode_s3_logs_lifecycle_rule_id',
            enabled=True, expiration=Duration.days(config_s3['logs_expiration']),
            prefix=config_s3['logs_root_path'],  #TODO: adapt to API
        )
        bucket = s3.Bucket(
            self, 'decode_s3_bucket_id', bucket_name=config_s3['bucket_name'],
            removal_policy=RemovalPolicy.DESTROY, auto_delete_objects=True,  #TODO: we probably don't want this in deployment
            lifecycle_rules=[logs_lifecycle_rule],
        )
        return bucket

    def get_api(self, config: dict):
        #TODO: https://www.eliasbrange.dev/posts/deploy-fastapi-on-aws-part-2-fargate-alb/
        pass

    def get_postprocessor(self, config: dict):
        """Updates DB after job finished (status+logs location), handles user notification, handles failures.
        """
        #TODO:
        pass
