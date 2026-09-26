#!/bin/bash

# Import Docker images inside the VM.
# Import Docker image tar archives uploaded from the host.

set -e

echo "=== Import Docker images ==="
echo "Start time: $(date)"

# Check the Docker service.
if ! systemctl is-active --quiet docker; then
    echo "Start the Docker service..."
    systemctl start docker
fi

# Image directory
IMAGES_DIR="/opt/kafka-docker/images/docker_images"

# Check that the image directory exists.
if [ ! -d "$IMAGES_DIR" ]; then
    echo "Error: Image directory does not exist: $IMAGES_DIR"
    echo "Copy the docker_images directory into the VM first"
    exit 1
fi

# Image archive list
IMAGE_FILES=(
    "zookeeper_3.8.3.tar"
    "bitnami_kafka_3.6.1.tar"
)

# Import images
for image_file in "${IMAGE_FILES[@]}"; do
    file_path="$IMAGES_DIR/$image_file"

    if [ -f "$file_path" ]; then
        echo "Importing image: $image_file"

        # Import images
        docker load -i "$file_path"

        # Show the import results.
        echo "✓ Image imported successfully: $image_file"
    else
        echo "⚠ Image archive does not exist: $file_path"
    fi
done

echo ""
echo "=== Verify the imported images ==="
echo "Imported images:"
docker images | grep -E "(zookeeper|kafka)" || echo "No matching images found"

echo ""
echo "=== Import complete ==="
echo "You can now run: /opt/kafka-docker/start-kafka.sh"
