#!/bin/bash

# Download Docker images locally.
# Save downloaded images as tar archives.

set -e

echo "=== Download Docker images ==="
echo "Start time: $(date)"

# Create the output directory.
OUTPUT_DIR="./docker_images"
mkdir -p $OUTPUT_DIR

# Image list
IMAGES=(
    "zookeeper:3.8.3"
    "bitnami/kafka:3.6.1"
)

# Download and save images.
for image in "${IMAGES[@]}"; do
    echo "Downloading image: $image"

    # Pull images
    docker pull $image

    # Generate filenames by replacing special characters.
    filename=$(echo $image | sed 's/[\/:]/_/g').tar

    # Save the image as a tar archive.
    echo "Saving image to: $OUTPUT_DIR/$filename"
    docker save -o "$OUTPUT_DIR/$filename" $image

    # Show file size
    size=$(du -h "$OUTPUT_DIR/$filename" | cut -f1)
    echo "Image size: $size"
done

echo ""
echo "=== Download complete ==="
echo "Image archive location: $OUTPUT_DIR/"
echo "File list:"
ls -lh $OUTPUT_DIR/

echo ""
echo "=== Steps to upload to a VM ==="
echo "1. Copy the $OUTPUT_DIR/ directory to the VM"
echo "2. Run inside the VM: /opt/kafka-docker/import-images.sh"
echo "3. Then run: /opt/kafka-docker/start-kafka.sh"
