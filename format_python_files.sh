#!/bin/bash

BLACK_PATH=$(which black)
if [ -z "$BLACK_PATH" ]; then
  echo "Black is not installed or not found in the environment."
  exit 1
fi

find . -name "*.py" -exec $BLACK_PATH {} +
echo "Formatting complete for all Python files."
