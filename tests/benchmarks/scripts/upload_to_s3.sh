#!/bin/bash
#
# Upload MS MARCO dataset files to S3 for RediSearch benchmarks
#
# Usage:
#   ./upload_to_s3.sh <dataset-name> <local-directory>
#
# Example:
#   ./upload_to_s3.sh 5M-msmarco-passages ./msmarco-5m-output
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
S3_BUCKET="s3://benchmarks.redislabs/redisearch/datasets"
ACL="public-read"

# Check arguments
if [ $# -ne 2 ]; then
    echo -e "${RED}Error: Invalid arguments${NC}"
    echo "Usage: $0 <dataset-name> <local-directory>"
    echo ""
    echo "Example:"
    echo "  $0 5M-msmarco-passages ./msmarco-5m-output"
    exit 1
fi

DATASET_NAME="$1"
LOCAL_DIR="$2"

# Validate local directory exists
if [ ! -d "$LOCAL_DIR" ]; then
    echo -e "${RED}Error: Directory not found: $LOCAL_DIR${NC}"
    exit 1
fi

# Check AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI not found${NC}"
    echo "Install with: pip install awscli"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}Error: AWS credentials not configured${NC}"
    echo "Configure with: aws configure"
    exit 1
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}MS MARCO Dataset S3 Upload${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Dataset Name: $DATASET_NAME"
echo "Local Directory: $LOCAL_DIR"
echo "S3 Destination: $S3_BUCKET/$DATASET_NAME/"
echo "ACL: $ACL"
echo ""

# List files to upload
echo -e "${YELLOW}Files to upload:${NC}"
find "$LOCAL_DIR" -type f -name "${DATASET_NAME}*" | while read file; do
    size=$(du -h "$file" | cut -f1)
    echo "  - $(basename "$file") ($size)"
done
echo ""

# Confirm upload
read -p "Continue with upload? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Upload cancelled"
    exit 0
fi

# Upload files
echo ""
echo -e "${YELLOW}Uploading files...${NC}"

aws s3 cp "$LOCAL_DIR/" \
    "$S3_BUCKET/$DATASET_NAME/" \
    --recursive \
    --acl "$ACL" \
    --exclude "*" \
    --include "${DATASET_NAME}*" \
    --no-progress

echo ""
echo -e "${GREEN}✓ Upload complete!${NC}"
echo ""

# Verify upload
echo -e "${YELLOW}Verifying upload...${NC}"
aws s3 ls "$S3_BUCKET/$DATASET_NAME/" --human-readable

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Upload Summary${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "S3 Location: $S3_BUCKET/$DATASET_NAME/"
echo ""
echo "Files are publicly accessible via HTTPS:"
for file in "$LOCAL_DIR"/${DATASET_NAME}*; do
    filename=$(basename "$file")
    url="https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/$DATASET_NAME/$filename"
    echo "  - $url"
done
echo ""
echo -e "${GREEN}✓ Ready for benchmark execution!${NC}"
echo ""

