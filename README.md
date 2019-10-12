# Vilago Support

Supporting data processing scripts for the Vilago crowdsourcing project

## Running

DirectRunner
```
docker run -w /host-pwd -v $PWD:/host-pwd --volumes-from gcp-config-vilago \
vilago-support \
python -m dataflow-get-signal-types directrunner --setup_file /host-pwd/setup.py --region us-west1 --project vilago-demo
```

Dataflow (Test)
```
docker run -w /host-pwd -v $PWD:/host-pwd --volumes-from gcp-config-vilago \
vilago-support \
python -m dataflow-get-signal-types dataflow-test --setup_file /host-pwd/setup.py --region us-west1 --project vilago-demo
```

Copyright 2019 © Andrew Nguyen