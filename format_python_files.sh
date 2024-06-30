#!/bin/bash

# Find all .py files and format them with black
find . -type f -name "*.py" -exec black {} \;

# Print a message indicating completion
echo "Formatting complete for all Python files."
