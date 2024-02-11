#!/usr/bin/env bash

ESCAPED_QUERY=$(printf '%s\n' "$1" | sed -e 's/[]\/$*.^[]/\\&/g')
ESCAPED_REPLACE=$(printf '%s\n' "$2" | sed -e 's/[]\/$*.^[]/\\&/g')
grep -rl "$1" . | xargs sed -i "s/$ESCAPED_QUERY/$ESCAPED_REPLACE/g"
