# Transform the dataset into actor-to-actor pairs and store in RDF format.
python3 transform_dataset.py

# Create Dgraph schema for actors.
curl http://alpha:8080/admin/schema --data "@schema.graphql"

# Load RDF data into Dgraph
dgraph live -f actors.rdf -a alpha:9080 -z zero:5080
