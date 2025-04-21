# generals-replay-parser

How to use:
<br>1- place replays in a folder, you can get replays from gentool using this tool: https://github.com/abdnh/generals-replay-search/blob/main/src/replays/gentool_downloader.py
<br>2- put `parse.py` in root directory of replays and run it 
<br>3- it will store parsed replays in `parsed` folder
<br>4- place `check_winner.py` in parsed folder and run it


added parseV2 with improved winner determination full credits to https://github.com/rhaivorn/replay-info
to use download both prng and parseV2, run parseV2
warning this script deletes duplicate replays and replays with invalid factions and AI players so either make a backup or modify the script
