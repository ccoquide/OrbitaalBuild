import os
import sys

### PATH to temporal directory related to node table with old IDs
PATH=sys.argv[1]
for file in os.listdir(f"{PATH}"):
    os.remove(f"{PATH}/{file}")
os.removedirs(f"{PATH}")