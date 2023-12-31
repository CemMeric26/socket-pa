#!/bin/bash

# Build the Docker image
docker build -t ceng435:latest .

# Start up the containers in the background
docker-compose up -d

# Execute a shell on the server container
# Note: These commands will not keep a shell open, they simply start one.
docker exec -it server /bin/bash

# Execute a shell on the client container
docker exec -it client /bin/bash

# If you want to keep the shell open on the server, remove the '&' from the previous command.
# If you want to keep the shell open on the client, comment out the server /bin/bash command and remove '&' from the client command.
