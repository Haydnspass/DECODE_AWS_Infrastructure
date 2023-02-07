# syntax=docker/dockerfile:1

# Base DECODE image
#TODO: will need to change
FROM decode

# Install cloud packages
ENV PATH="/venv/bin:$PATH"
RUN pip3 install boto3 cloudpathlib

# Copy in entrypoint
COPY . .
RUN chmod +x /entrypoint.sh

# Run entrypoint
# Container needs a role on AWS,
# see https://hands-on.cloud/working-with-aws-batch-in-python-using-boto3/#AWS-Batch-Job-IAM-role;
# locally, pass credentials as -v ${HOME}/.aws/credentials:/root/.aws/credentials:ro
ENTRYPOINT ["/entrypoint.sh"]
