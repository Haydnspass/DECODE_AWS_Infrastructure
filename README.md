# DECODE_AWS_Infrastructure

## Setup
 - Install nodejs (v>=14) and npm.
 - `npm install -g aws-cdk`
 - (done only once) `mkdir cloud_formation && cd cloud_formation && cdk init --language python app`
 - cdk --profile nolan --context profile=nolan synth/deploy/destroy
