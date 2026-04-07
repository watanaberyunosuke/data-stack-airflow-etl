#!/bin/bash

# Function to check if a command exists
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Check for 'black' installation
if ! command_exists black; then
  echo "Black is not installed or not found in the environment."
  exit 1
fi

# Check for 'sqlfluff' installation
if ! command_exists sqlfluff; then
  echo "sqlfluff is not installed or not found in the environment."
  exit 1
fi

# Format Python files
find . -name "*.py" -exec black {} +
echo "Formatting complete for all Python files."

# Format SQL files
find . -name "*.sql" -exec sqlfluff fix --force {} +
echo "Formatting complete for all SQL files."
