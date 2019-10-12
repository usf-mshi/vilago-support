FROM andrewnguyensf/docker-beam:2.16.0

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

RUN apt-get install apt-transport-https ca-certificates

RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

RUN apt-get update && apt-get -y install google-cloud-sdk

RUN pip install pyedflib
RUN pip install google-cloud-storage
RUN pip install click