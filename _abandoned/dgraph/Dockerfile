FROM dgraph/dgraph:v20.07.1
RUN apt update && \
    apt install -y python3 && \
    apt install -y python3-pip
RUN pip3 install rdflib
WORKDIR /app
ADD dataset/ dataset/
ADD init.sh .
ADD schema.graphql .
ADD transform_dataset.py .
