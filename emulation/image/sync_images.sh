#!/bin/bash

# Sync Docker images to multiple machines
# Upload docker_images and import_images.sh to the selected VM.

set -e

echo "=== Sync Docker images to multiple machines ==="
echo "Start time: $(date)"

# Source directory
SOURCE_DIR="docker_images"
TARGET_DIR="/opt/kafka-docker/images/docker_images"
IMPORT_SCRIPT="import_images.sh"
IMPORT_SCRIPT_TARGET="/opt/kafka-docker/import_images.sh"

# Target machine list
HOSTS=(
    "192.168.100.3"
    "192.168.100.4"
    "192.168.100.5"
)

# Check that the source directory exists.
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directorydoes not exist: $SOURCE_DIR"
    echo "Ensure the docker_images directory is in the current directory"
    exit 1
fi

# Check whether import_images.sh exists
if [ ! -f "$IMPORT_SCRIPT" ]; then
    echo "Error: import_images.shdoes not exist: $IMPORT_SCRIPT"
    echo "Ensure import_images.sh is in the current directory"
    exit 1
fi

# Inspect the source directory contents.
echo "Source directory contents:"
ls -la "$SOURCE_DIR"

echo "Import script:"
ls -la "$IMPORT_SCRIPT"

echo ""
echo "Starting sync to target machines..."

# Iterate over target machines
for host in "${HOSTS[@]}"; do
    echo ""
    echo "=== Syncing to $host ==="

    # Check target machine connectivity
    if ! ping -c 1 -W 3 "$host" >/dev/null 2>&1; then
        echo "⚠ Warning: Cannot connect to $host, skipped"
        continue
    fi

    # Create directory on target machine
    echo "Creating target directory..."
    ssh root@"$host" "mkdir -p $TARGET_DIR"
    ssh root@"$host" "mkdir -p /opt/kafka-docker"

    # Sync Docker image files
    echo "Sync Docker image files..."
    rsync -avz --progress \
        --exclude='*.tmp' \
        --exclude='*.log' \
        "$SOURCE_DIR/" \
        "root@$host:$TARGET_DIR/"

    # Sync import_images.sh script
    echo "Sync import_images.sh script..."
    rsync -avz --progress \
        "$IMPORT_SCRIPT" \
        "root@$host:$IMPORT_SCRIPT_TARGET"

    # Set script execute permission
    ssh root@"$host" "chmod +x $IMPORT_SCRIPT_TARGET"

    if [ $? -eq 0 ]; then
        echo "✓ Sync succeeded: $host"

        # Verify sync result
        echo "Verify Docker images directory:"
        ssh root@"$host" "ls -la $TARGET_DIR"

        echo "Verify the import script:"
        ssh root@"$host" "ls -la $IMPORT_SCRIPT_TARGET"

        # Show file size
        echo "Target directory size:"
        ssh root@"$host" "du -sh $TARGET_DIR"
    else
        echo "✗ Sync failed: $host"
    fi
done

echo ""
echo "=== Sync complete ==="
echo "End time: $(date)"

# Show status for all target machines
echo ""
echo "=== All target machine status ==="
for host in "${HOSTS[@]}"; do
    echo ""
    echo "--- $host ---"
    if ping -c 1 -W 3 "$host" >/dev/null 2>&1; then
        echo "Connection status: ✓ online"
        echo "Docker images directory contents:"
        ssh root@"$host" "ls -la $TARGET_DIR 2>/dev/null || echo 'Directory does not exist'"
        echo "Import script:"
        ssh root@"$host" "ls -la $IMPORT_SCRIPT_TARGET 2>/dev/null || echo 'Script does not exist'"
    else
        echo "Connection status: ✗ offline"
    fi
done

echo ""
echo "Sync complete!You can now run this on the target machines:"
echo "  /opt/kafka-docker/import_images.sh"
