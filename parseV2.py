import prng
import re
from datetime import datetime, timedelta, timezone, UTC
import time
import struct
import requests
from io import BytesIO
import hashlib
import glob
import os
import sqlite3
import matplotlib
# Try setting backend before importing pyplot
try:
    matplotlib.use('Agg') # Use Agg backend for non-interactive plotting
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("Warning: matplotlib not found. Charts will not be generated.")
    print("Install it using: pip install matplotlib")

# Import numpy if matplotlib is available
if MATPLOTLIB_AVAILABLE:
    try:
        import numpy as np
    except ImportError:
        print("Warning: numpy not found. Charts might not be generated correctly.")
        print("Install it using: pip install numpy")
        # Disable charting if numpy is missing, as it's needed for positioning
        MATPLOTLIB_AVAILABLE = False


from multiprocessing import Pool, cpu_count # Import multiprocessing
from functools import partial # For passing arguments to pool workers

# --- Constants ---
DB_FILE = "replay_stats.db"

# Valid versions (case-insensitive comparison will be used)
VALID_VERSIONS = {"Version 1.04", "버전 1.04", "版本 1.04", "Версия 1.04", "Versión 1.04", "Versione 1.04"}
VALID_VERSIONS_LOWER = {v.lower() for v in VALID_VERSIONS} # Pre-compute lowercase set

# Allowed "pro" maps (case-insensitive comparison will be used)
ALLOWED_MAPS = {
    "Alpine Assault", "Barren Badlands", "Bitter Winter", "Bombardment Beach", "Desert Fury", "Dust Devil",
    "Final Crusade", "Flash Fire", "Forgotten Forest", "Heartland Shield", "Killing Fields", "Leipzig Lowlands",
    "North America", "Sand Serpent", "Seaside Mutiny", "Silent River", "The Frontline", "Tournament Desert",
    "Tournament Plains", "Wasteland Warlords", "Winding River", "Winter Wolf", "[RANK] Antarctic Lagoon ZH v3",
    "[RANK] Barren Badlands Balanced ZH v1", "[RANK] Blizzard Badlands Reloaded", "[RANK] Blizzard Badlands ZHv5",
    "[RANK] Bounty v3", "[RANK] Canyon of the Dead v1", "[RANK] Coastal Conflict ZH v2", "[RANK] Desert Quadrant ZH v1",
    "[RANK] Early Spring ZH v1", "[RANK] Evergreen Lagoon", "[RANK] Final Crusade Balanced ZH v1",
    "[RANK] Final Crusade FIXEDv2", "[RANK] Flash Fire Balanced ZH v1", "[RANK] Forbidden Takover ZH v2",
    "[RANK] Forbidden Takover ZH v3", "[RANK] Forgotten Air Battle v2", "[RANK] Forgotten Air Battle v4",
    "[RANK] Forgotten Ruins", "[RANK] Frog Prince ZH v2", "[RANK] Frozen Ruins", "[RANK] Highway 99 ZH v1",
    "[RANK] Homeland Rocks ZH v3", "[RANK] Jungle Wolf ZH v1", "[RANK] Jungle Wolf ZH v2", "[RANK] Lagoon ZH v2",
    "[RANK] Lagoon ZH v4", "[RANK] Liquid Gold ZH v1", "[RANK] Make Make 2 ZH v3", "[RANK] Melting Snow ZH v2",
    "[RANK] Melting Snow ZH v3", "[RANK] Natural Threats ZH v2", "[RANK] Sand Serpent Balanced ZH v1",
    "[RANK] Sand Serpent FIXED", "[RANK] Silicon Valley ZH v1", "[RANK] Snow Blind ZH v1", "TD Classic ZH v1",
    "TD NoBugs ZH v1", "TD OpenMiddle NoCars ZH v1", "TD OpenMiddle ZH v1", "[RANK] Abandoned Desert ZH v1",
    "[RANK] Ammars Sandcastles v3", "[RANK] Annihilation", "[RANK] Area J1", "[RANK] Arena of War ZH v1",
    "[RANK] Artic Lagoon", "[RANK] Australia ZH v1", "[RANK] Bitter Winter Balanced NoCars ZH v1",
    "[RANK] Bozic Destruction ZH v3", "[RANK] Cold Territory ZH v2", "[RANK] Danger Close ZH", "[RANK] DeDuSu ZH v1",
    "[RANK] Deserted Village v3", "[RANK] Desolated District ZH v1", "[RANK] Devastated Oasis ZH v2",
    "[RANK] Eagle Eye", "[RANK] Eight ZH v2", "[RANK] Embattled Land ZH v2", "[RANK] Forest of Oblivion ZH v1",
    "[RANK] Gold Cobra", "[RANK] Hard Winter ZH v2", "[RANK] Hidden Treasures v2", "[RANK] Homeland Rocks ZH v4",
    "[RANK] Irish Front ZH v1", "[RANK] Liquid Gold ZH v2", "[RANK] Lost Valley v2", "[RANK] Mountain Mayhem v2",
    "[RANK] Mountain Oil ZH v1", "[RANK] Natural Threats ZH v3", "[RANK] Onza Map v1", "[RANK] Rebellion ZH v1",
    "[RANK] Sand Scorpion", "[RANK] Scaraa ZH v1", "[RANK] Scorched Earth ZH v3", "[RANK] Sleeping Dragon v3",
    "[RANK] Snow Aggression v3", "[RANK] Snow Blind ZH v2", "[RANK] Snowy Drought v4", "[RANK] Storm Valley",
    "[RANK] TD Classic NoCars ZH v1", "[RANK] TD NoBugsCars ZH v1", "[RANK] Total Domination No SDZ ZH v1",
    "[RANK] Tournament Delta ZH v2", "[RANK] Uneven Heights v3", "[RANK] Vendetta ZH v1",
    "[RANK] Wasteland Warlords Revised", "[RANK] Winter Arena", "[RANK] Winter Wolf Balanced ZH v1",
    "[RANK] ZH Carrier is Over v2", "[RANK] [NMC] Battle on the River", "[RANK] [NMC] Blasted Lands",
    "[RANK] [NMC] Summer Arena", "[RANK] [NMC] Tournament Arena", "[RANK] [NMC] Tournament City",
    "[RANK] A New Tragedy ZH v1", "[RANK] Abandoned Farms ZH v1", "[RANK] Barren Badlands Balanced ZH v2",
    "[RANK] Battle Plan ZH v1", "[RANK] Blossoming Valley ZH v1", "[RANK] Canyon of the Dead ZH v2",
    "[RANK] Combat Island ZH v1", "[RANK] Down the Road ZH v1", "[RANK] Drallim Desert ZH v2",
    "[RANK] Early Spring ZH v2", "[RANK] Egyptian Oasis ZH v1", "[RANK] Farmlands of the Fallen ZH v1",
    "[RANK] Final Crossroad ZH v1", "[RANK] Forgotten Air Battle ZH v5", "[RANK] A New Tragedy ZH v2",
    "[RANK] AKAs Magic ZH v1", "[RANK] Alfies Haven ZH v1", "[RANK] Arctic Arena ZH v1", "[RANK] Arctic Lagoon ZH v2",
    "[RANK] Arizona Airfield ZH v1", "[RANK] Battleship Bay ZH v1", "[RANK] Black Hell ZH v1", "[RANK] Blue Hole ZH v1",
    "[RANK] Dammed Cottages ZH v1", "[RANK] Dammed Korhal ZH v1", "[RANK] Dammed Scorpion ZH v1",
    "[RANK] Desert Fury ZH v1", "[RANK] Double Damination ZH v1", "[RANK] Dry River ZH v1", "[RANK] Dust Devil ZH v1",
    "[RANK] Echo ZH v1", "[RANK] Endboss ZH v1", "[RANK] Fort Payne ZH v1", "[RANK] Hanamura Temple ZH v1",
    "[RANK] Natural Threats ZH v4", "[RANK] Proxy War ZH v1", "[RANK] Rainforest Reservoir ZH v1",
    "[RANK] Sacred Land ZH v1", "[RANK] Sakura Forest II ZH v1", "[RANK] Salt Lake River ZH v1",
    "[RANK] Snowy Drought ZH v5", "[RANK] Snowy Roads ZH v1", "[RANK] Storm Surge ZH v1", "[RANK] Tiny Tactics ZH v1",
    "[RANK] Winding River Revised ZH v1", "[RANK] Yelling Avalanche ZH v1", "1v1 try it_v2a", "1v1 try it_v2b",
    "Additional Forces ZH v1", "Alpine Assault v2", "Battle Park", "Bitter Winter Balanced ZH vB", "ButterbroT ZH v3",
    "Canyon Frost ZH v1", "Crazy Beach ZH v1", "Cross-Country", "Down the Road v3", "Drallim Desert v1",
    "Entropys Empire ZH v2", "Forest of Camelot ZH v1", "Freezing Rain v1", "Frozen Dawn ZH v1 (draf02)",
    "GenTools secret Lab A", "Jammed Lands {v3}", "Killing Fields Balanced v2", "Kinky Fields ZH v1",
    "Leipzig Lowlands Balanced ZH vB", "Lone Outpost", "Make-Make ZHv2", "Modern Warfare ZH v0",
    "Mountain Arena ZH v1", "Poseidons Lair ZH v2", "Rising Legion ZH v1", "Salt Lake River ZH v1", "Siege(Tower)",
    "Sleeping Dragon V2", "Snow Land Nation ZH v3", "Snowy Plateau v2", "Stonehenge ZH TEST v4",
    "Tournament in Canyon V2", "Tournament in Canyon V32", "Urban Pinch ZH v1", "Wings of Fury",
    "Wrong Neighborhood v1", "Yota Nation Arena ZH v1", "Yota Nation Battleground ZH v1",
    "[RANK] Bozic Destruction ZH v4", "[RANK] Tournament Delta ZH v3"
}
ALLOWED_MAPS_LOWER = {m.lower() for m in ALLOWED_MAPS} # Pre-compute lowercase set

# --- Helper Functions ---

def read_null_terminated_string(file, encoding='utf-8', return_is_corrupt=False):
    """Reads a null-terminated string from a file object."""
    chars = []
    try:
        if encoding == 'utf-16':
            null_char = b'\x00\x00'; char_size = 2
        else:
            null_char = b'\x00'; char_size = 1
        while True:
            char_bytes = file.read(char_size)
            if not char_bytes or len(char_bytes) < char_size or char_bytes == null_char: break
            chars.append(char_bytes)
        byte_data = b''.join(chars); is_corrupt = False
        try: result = byte_data.decode(encoding)
        except UnicodeDecodeError:
            is_corrupt = True
            try: result = byte_data.decode('cp1252')
            except UnicodeDecodeError:
                try: result = byte_data.decode('latin-1')
                except UnicodeDecodeError: result = byte_data.decode('utf-8', errors='ignore'); is_corrupt = True
    except Exception as e:
        print(f"  Error reading null-terminated string: {e}")
        if return_is_corrupt: return None, True
        else: return None
    return (result, is_corrupt) if return_is_corrupt else result

def check_encoding_bytes(input_bytes):
    """Checks if bytes correctly encode/decode as UTF-8."""
    try:
        decoded_content = input_bytes.decode('utf-8')
        encoded_bytes = decoded_content.encode('utf-8')
        return input_bytes != encoded_bytes
    except UnicodeDecodeError: return True
    except Exception: return True

def is_valid_utf8(byte_string):
    """Checks if a byte string is valid UTF-8."""
    try: byte_string.decode('utf-8'); return True
    except UnicodeDecodeError: return False

def hex_to_decimal(hex_string):
    """Converts little-endian hex string to decimal integer."""
    try:
        if len(hex_string) % 2 != 0: hex_string = '0' + hex_string
        return int.from_bytes(bytes.fromhex(hex_string), byteorder='little', signed=False)
    except (ValueError, TypeError): return 0

def assign_random_faction(game_prng, total_factions, game_sd):
    """Assigns a random faction using the game's PRNG logic."""
    discard = game_sd % 7
    for _ in range(discard): game_prng.get_value(0, 1)
    return game_prng.get_value(0, 1000) % total_factions

def assign_random_color(game_prng, total_colors, taken_colors):
    """Assigns a random, untaken color using the game's PRNG logic."""
    color_index = -1; attempts = 0
    while color_index == -1 and attempts < total_colors * 2:
        random_color = game_prng.get_value(0, total_colors - 1)
        if not taken_colors[random_color]: color_index = random_color; taken_colors[random_color] = True
        attempts += 1
    if color_index == -1: # Fallback
        for i in range(total_colors):
            if not taken_colors[i]: color_index = i; taken_colors[i] = True; break
    return color_index

def ddhhmmss(seconds):
    """Formats seconds into DD:HH:MM:SS.ff or shorter formats."""
    if not isinstance(seconds, (int, float)) or seconds < 0: return '00s 00f'
    if seconds == 0: return '00s 00f'
    try:
        days, rem = divmod(seconds, 86400); hours, rem = divmod(rem, 3600)
        mins, secs = divmod(rem, 60); int_secs = int(secs)
        frames = int(round((secs - int_secs) * 30))
        if frames >= 30: int_secs += 1; frames = 0 # Rollover logic
        if int_secs >= 60: mins += 1; int_secs = 0
        if mins >= 60: hours += 1; mins = 0
        if hours >= 24: days += 1; hours = 0
        days, hours, mins = int(days), int(hours), int(mins)
        if days > 0: return f"{days:02}:{hours:02}:{mins:02}:{int_secs:02}.{frames:02}"
        if hours > 0: return f"{hours:02}:{mins:02}:{int_secs:02}.{frames:02}"
        if mins > 0: return f"{mins:02}m {int_secs:02}s {frames:02}f"
        return f"{int_secs:02}s {frames:02}f"
    except Exception: return '??s ??f'

def get_replay_data(filename, mode):
    """Gets replay data from local file or URL."""
    header = None; data = None
    try:
        if mode == 1:
            with open(filename, 'rb') as f: header, data = parse_replay_data(f)
        elif mode == 2:
            response = requests.get(filename, timeout=10); response.raise_for_status()
            with BytesIO(response.content) as f: header, data = parse_replay_data(f)
        else: print(f"Invalid mode for get_replay_data: {mode}")
    except FileNotFoundError: print(f"Error: Replay file not found: {filename}")
    except requests.exceptions.RequestException as e: print(f"Error retrieving online replay {filename}: {e}")
    except Exception as e: print(f"Error processing replay data for {filename}: {e}")
    return header or {}, data or ""

def parse_replay_data(f):
    """Parses the replay header and returns header dict and hex data string."""
    magic = f.read(6)
    if magic != b'GENREP': raise ValueError("Invalid replay file format")
    begin_timestamp, end_timestamp, replay_duration = struct.unpack('<III', f.read(12))
    desync, early_quit = struct.unpack('<BB', f.read(2)); disconnect = struct.unpack('<8B', f.read(8))
    file_name = read_null_terminated_string(f, encoding='utf-16')
    system_time_raw = struct.unpack('<8H', f.read(16))
    version = read_null_terminated_string(f, encoding='utf-16') # Read version string
    build_date = read_null_terminated_string(f, encoding='utf-16')
    version_minor, version_major = struct.unpack('<HH', f.read(4))
    exe_crc, ini_crc = struct.unpack('<II', f.read(8))
    match_data, is_corrupt = read_null_terminated_string(f, return_is_corrupt=True)
    if match_data is None: match_data = ""
    local_player_index_str = read_null_terminated_string(f)
    try: local_player_index = int(local_player_index_str) if local_player_index_str else -1
    except ValueError: local_player_index = -1
    difficulty, original_game_mode, rank_points, max_fps = struct.unpack('<iiii', f.read(16))
    hex_data = f.read().hex()
    header = {
        "magic": magic, "begin_timestamp": begin_timestamp, "end_timestamp": end_timestamp,
        "replay_duration": replay_duration, "desync": desync, "early_quit": early_quit,
        "disconnect": disconnect, "file_name": file_name or "", "system_time": system_time_raw,
        "version": version or "", "build_date": build_date or "", "version_minor": version_minor, # Store version
        "version_major": version_major, "exe_crc": exe_crc, "ini_crc": ini_crc,
        "match_data": match_data, "local_player_index": local_player_index, "difficulty": difficulty,
        "original_game_mode": original_game_mode, "rank_points": rank_points, "max_fps": max_fps,
        "is_corrupt": is_corrupt,
    }
    return header, hex_data

def fix_empty_slot_issue(slots_data):
    """Maps original slot index to occupied slot index."""
    initial_indices = {}; occupied_idx = 0
    for i, slot in enumerate(slots_data):
        if slot not in ('X', 'O', ''): initial_indices[i] = occupied_idx; occupied_idx += 1
    return initial_indices

def get_pl_num_offset(player_slot, slots_data, hex_data):
    """Determine the player number offset based on CRC messages."""
    fixed_slots = fix_empty_slot_issue(slots_data)
    if player_slot not in fixed_slots:
         if 0 in fixed_slots: player_slot = 0 # Fallback: assume first player if local slot invalid
         else: return 2, 2, False # Default offset if truly lost
    offset = 2; pl_num_from_first_crc = []
    index_of_first_crc = hex_data.find("00470400000") # Find first logic CRC start
    if index_of_first_crc != -1 and index_of_first_crc >= 8: # Need space for frame
        frame_hex = hex_data[index_of_first_crc-8:index_of_first_crc] # Get frame before CRC msg
        pattern = f"{frame_hex}470400000." # Match frame, type, and the player number hex digit
        found_crcs = set(re.findall(pattern, hex_data))
        if len(found_crcs) >= len(fixed_slots): # Check if we found at least enough CRCs
            try:
                sorted_crc_msgs = sorted(list(found_crcs))
                pl_num_from_first_crc = [int(crc_msg[-1], 16) for crc_msg in sorted_crc_msgs]
                if pl_num_from_first_crc:
                    offset = pl_num_from_first_crc[0] # Assume lowest player number found corresponds to offset
                    num_player = offset + fixed_slots[player_slot]
                    # Check end-of-game message pattern
                    if hex_data.endswith(f"1b0000000{num_player:x}00000000"): return num_player, offset, True
                    if hex_data[-18:-10] == '1b000000': # End pattern exists but player num differs
                         end_num = int(hex_data[-9:-8], 16)
                         num_player = end_num; offset = num_player - fixed_slots[player_slot]
                         return num_player, offset, True
                    return num_player, offset, False # No clear end pattern, rely on CRC offset
            except (ValueError, IndexError): pass # Fallback on error
    # Fallback if not enough CRC messages found or initial find failed
    offset = 2; num_player = offset + fixed_slots.get(player_slot, 0)
    if hex_data[-18:-10] == '1b000000': # Check end pattern as a last resort
        try:
            num_player = int(hex_data[-9:-8], 16); offset = num_player - fixed_slots.get(player_slot, 0)
            return num_player, offset, True
        except (ValueError, IndexError): pass
    return num_player, offset, False

def comp_name(comp_char):
    """Returns AI difficulty name from character."""
    if comp_char == 'E': return 'Easy AI'
    if comp_char == 'M': return 'Medium AI'
    if comp_char == 'H': return 'Hard AI'
    return 'Unknown AI'

def fix_teams(teams, players):
    """Assigns unique team IDs if team 0 (FFA?) is used."""
    if 0 in teams:
        taken_keys = set(teams.keys()); next_new_key = (max(taken_keys) if taken_keys else 0) + 1
        players_without_team = teams.pop(0, [])
        for player_num in players_without_team:
            while next_new_key in taken_keys: next_new_key += 1
            teams[next_new_key] = [player_num]
            if player_num in players: players[player_num]['team'] = next_new_key
            taken_keys.add(next_new_key); next_new_key += 1
    return dict(sorted(teams.items())), players

def get_match_type(teams):
    """Generates match type string like '1v1', '2v2', '1v1v1'."""
    if not teams: return "Unknown"
    team_sizes = [len(p) for p in teams.values() if p] # Count players per team
    if not team_sizes: return "Empty"
    return 'v'.join(map(str, sorted(team_sizes)))

def find_winning_team(teams_data):
    """Determines the winning team based on player quit/end times."""
    teams_remain = []; teams_quit = []; found_winner = False; winning_team = 0
    if not teams_data: return found_winner, winning_team
    for team, player_times in teams_data.items():
        if not player_times: continue
        if -1 in player_times: teams_remain.append(team) # Team has players who didn't quit
        else:
            valid_times = [t for t in player_times if isinstance(t, (int, float)) and t >= 0]
            if valid_times: teams_quit.append((team, max(valid_times))) # All players quit, record last quit time
            elif -1 not in player_times: teams_remain.append(team) # No -1 and no valid times? Treat as remaining.
    if len(teams_remain) == 1: return True, teams_remain[0] # Exactly one team remains
    if len(teams_remain) > 1: return False, 0 # Multiple teams remain (draw/desync?)
    if teams_quit: teams_quit.sort(key=lambda x: x[1], reverse=True); return True, teams_quit[0][0] # All quit, latest quitter wins
    return False, 0 # No teams remaining or quit (empty?)

def extract_frame(hex_data, index):
    """Extracts frame value (4 bytes, little-endian) before a message index."""
    if index < 8 or index > len(hex_data): return 0
    return hex_to_decimal(hex_data[index-8:index])

def extract_crc(hex_data, index):
    """Extracts CRC value (4 bytes, little-endian) from a CRC message."""
    crc_start_index = index + 30 # Offset from start of 4704 msg to CRC value
    if crc_start_index + 8 > len(hex_data): return 0
    return hex_to_decimal(hex_data[crc_start_index : crc_start_index+8])

def update_players_data(num_player, hex_data, quit_data, teams, teams_data, winning_team, players_quit_frames, observer_num_list, last_crc_data, last_crc_index, found_winner):
    """Determines surrender/exit/idle times based on quit messages and CRC data."""
    max_losers_index = -1 # Index of the latest quit message among losers
    if found_winner and len(teams) > 1:
        try:
            valid_loser_times = [max(times) for team, times in teams_data.items() if team != winning_team and any(isinstance(t, (int, float)) and t >= 0 for t in times)]
            if valid_loser_times: max_losers_index = max(valid_loser_times)
        except ValueError: max_losers_index = -1

    # Update last_crc based on owner's last known CRC frame
    if last_crc_index > 0:
        for player_num, player_data in players_quit_frames.items():
            if player_num in last_crc_data:
                player_quit_time = quit_data.get(player_num, [-1])[0]
                # Player's CRC is valid if they didn't quit before owner's last CRC frame
                if player_quit_time == -1 or last_crc_index < player_quit_time:
                     crc_val = extract_crc(hex_data, last_crc_data[player_num])
                     if crc_val != 0: player_data['last_crc'] = crc_val

    # Process quit messages (4504 type)
    for player_num, player_data in players_quit_frames.items():
        if player_num not in quit_data: continue
        quit_indices = quit_data[player_num]
        frame_time = extract_frame(hex_data, quit_indices[0]) # Time of first quit message
        if len(quit_indices) > 1: # Multiple quits -> Surrender then Exit
            player_data['surrender'] = frame_time
            player_data['exit'] = extract_frame(hex_data, quit_indices[1]); continue
        # Single Quit Message Logic:
        if player_num in observer_num_list: player_data['exit'] = frame_time; continue # Observers just exit
        if player_num in last_crc_data and last_crc_index > quit_indices[0]: player_data['surrender'] = frame_time; continue # Sent CRC after quit -> Surrender
        if found_winner and len(teams) > 1 and max_losers_index > -1 and (player_num in teams.get(winning_team, [])):
             if frame_time > max_losers_index: player_data['exit'] = frame_time; continue # Winner quit after last loser -> Exit
        if num_player in quit_data: # Compare to owner's quit
            owner_last_activity_index = max(quit_data[num_player])
            if quit_indices[0] < owner_last_activity_index: # Quit before owner finished
                 owner_crc_after_player_quit = hex_data.find(f"470400000{num_player:x}", quit_indices[0])
                 if owner_crc_after_player_quit != -1 and owner_crc_after_player_quit < owner_last_activity_index: player_data['surrender'] = frame_time # Owner active after -> Surrender
                 else: player_data['surrender/exit?'] = frame_time # Ambiguous
                 continue
            else: player_data['exit'] = frame_time; continue # Quit after owner finished -> Exit
        player_data['exit'] = frame_time # Default: single quit is exit

def get_match_mode(host_hex_ip, host_port):
    """Determines match mode (Online, LAN, GameRanger, Skirmish)."""
    try:
        host_hex_ip, host_port = str(host_hex_ip), str(host_port)
        if not host_hex_ip or not host_port: return 'Unknown (Invalid Host Info)'
        host_ip_dec = int(host_hex_ip, 16)
    except (ValueError, TypeError): return 'Unknown (Invalid IP Format)'
    if host_ip_dec == 0 and host_port == '0': return 'Skirmish/Offline'
    lan_ranges = [(0x0A000000, 0x0AFFFFFF), (0xAC100000, 0xAC1FFFFF), (0xC0A80000, 0xC0A8FFFF), (0xA9FE0000, 0xA9FEFFFF), (0x1A000000, 0x1AFFFFFF), (0x19000000, 0x19FFFFFF)]
    is_lan_ip = any(start <= host_ip_dec <= end for start, end in lan_ranges)
    if host_port == '8088': return 'LAN (VPN/GR? Port 8088)' if is_lan_ip else 'GameRanger' # Port 8088 often GR
    if is_lan_ip: return 'LAN'
    return 'Online'

def ordinal(n):
    """Returns the ordinal string for a number (1st, 2nd, 3rd, etc.)."""
    if not isinstance(n, int) or n <= 0: return ""
    if 11 <= (n % 100) <= 13: return f"{n}th"
    return f"{n}{ {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th') }"

def sanitize_filename(filename, replacement="_"):
    """Removes invalid characters for filenames."""
    if not isinstance(filename, str): filename = str(filename)
    invalid_chars = r'[<>:"/\\|?*\x00-\x1F]' # Added control characters
    cleaned = re.sub(invalid_chars, replacement, filename).strip(" .")
    cleaned = re.sub(f'{replacement}+', replacement, cleaned) # Consolidate replacements
    if not cleaned: return "sanitized_filename"
    max_len = 200 # Limit length
    if len(cleaned) > max_len:
        base, dot, ext = cleaned.rpartition('.')
        if dot and len(ext) < 10: cleaned = base[:max_len - len(ext) - 1] + dot + ext
        else: cleaned = cleaned[:max_len]
    return cleaned

def string_to_md5(input_string):
    """Generates MD5 hash for a string."""
    return hashlib.md5(input_string.encode('utf-8')).hexdigest()

def get_match_id(url, start_time, game_sd, match_type, map_crc, player_nicks):
    """Generates a unique match ID (placeholder logic)."""
    date_in_replay = datetime.fromtimestamp(start_time, UTC).date(); date_uploaded = date_in_replay
    match = re.search(r'/(\d{4})_(\d{2})_[^/]+/(\d{2})_', url or "")
    if match:
        try: year, month, day = map(int, match.groups()); date_uploaded = datetime(year, month, day).date()
        except ValueError: pass
    two_days_ago = date_uploaded - timedelta(days=2)
    player_string = "".join(sorted(player_nicks)); base_string = f"{game_sd}{match_type}{map_crc}{player_string}"
    date_str = date_in_replay.strftime('%Y%m%d') if date_in_replay >= two_days_ago else date_uploaded.strftime('%Y%m%d')
    return string_to_md5(f"{date_str}{base_string}")


# --- Main Replay Parsing Logic ---

def get_replay_info(file_path, mode, rename_info=False):
    """Parses a Generals Zero Hour replay file (local or online)."""
    try:
        header, hex_data = get_replay_data(file_path, mode)
        if not header or not hex_data: return None if not rename_info else "parsing_failed"

        # --- Extract Core Header Info ---
        start_time = header.get('begin_timestamp', 0); rep_duration_header = header.get('replay_duration', 0)
        player_slot = header.get('local_player_index', -1); desync = header.get('desync', 0)
        disconnect_flags = header.get('disconnect', (0,)*8); version = header.get('version', 'Unknown')
        build_date = header.get('build_date', 'Unknown'); exe_crc = header.get('exe_crc', 0); ini_crc = header.get('ini_crc', 0)
        match_data_str = header.get('match_data', ""); is_corrupt_flag = header.get('is_corrupt', False)

        # --- Version Check ---
        if version.lower() not in VALID_VERSIONS_LOWER:
             # This function is now only called in Pass 2, Pass 1 already filters
             # print(f"Warning: Invalid version '{version}' found in Pass 2 for {file_path}")
             # We might still want to process it if Pass 1 let it through, or return error?
             # Let's assume Pass 1 filtering is sufficient and proceed.
             pass

        # --- Parse Match Data String ---
        match_data = {}; current_key = None; accumulated_value = ""
        if match_data_str:
            parts = match_data_str[:-2].split(';') if match_data_str.endswith(';;') else match_data_str.split(';')
            for part in parts:
                if '=' in part:
                    if current_key is not None: match_data[current_key] = accumulated_value
                    key_value = part.split('=', 1); current_key = key_value[0]; accumulated_value = key_value[1]
                elif current_key is not None: accumulated_value += ';' + part
            if current_key is not None: match_data[current_key] = accumulated_value

        slots_raw_str = match_data.get('S', ''); slots_data = re.split(r':(?=[HCXO])', slots_raw_str) if slots_raw_str else []
        map_crc = match_data.get('MC', 'Unknown')
        map_full_path = match_data.get('M', 'Unknown Map'); map_name = map_full_path[map_full_path.rfind('/')+1:] if '/' in map_full_path else map_full_path
        try: game_sd = int(match_data.get('SD', '0'))
        except ValueError: game_sd = 0
        start_cash = match_data.get('SC', '10000'); sw_restriction_val = match_data.get('SR', '0'); sw_restriction = 'Yes' if sw_restriction_val == '1' else 'No'

        # --- Determine Player Number Offset ---
        num_player, offset, is_normal_rep = get_pl_num_offset(player_slot, slots_data, hex_data)

        # --- Initialize Player/Team Data ---
        players = {}; teams = {}; player_nicks = []; observer_num_list = []; player_num_list = []
        computer_player_in_game = False; host_hex_ip = '0'; host_port = '0'
        colors = { -1: 'Random', 0: 'Gold', 1: 'Red', 2: 'Blue', 3: 'Green', 4: 'Orange', 5: 'Cyan', 6: 'Purple', 7: 'Pink' }
        factions = { -2: 'Observer', -1: 'Random', 0: 'USA', 1: 'China', 2: 'GLA', 3: 'USA SW', 4: 'USA Laser', 5: 'USA Air', 6: 'China Tank', 7: 'China Inf', 8: 'China Nuke', 9: 'GLA Toxin', 10: 'GLA Demo', 11: 'GLA Stealth' }
        rename_factions = { -2: 'obs', -1: 'random', 0 : 'usa', 1: 'china', 2: 'gla', 3: 'sw', 4: 'laser', 5: 'air', 6: 'tank', 7: 'inf', 8: 'nuke', 9: 'tox', 10: 'demo', 11: 'stealth' }

        # --- Parse Slot Data ---
        pl_count = 0
        for index, player_raw in enumerate(slots_data):
            if not player_raw or player_raw[0] in ('X', 'O'): continue
            player_data_parts = player_raw.split(','); player_type = player_raw[0]; player_num_current = pl_count + offset
            try:
                if player_type == 'H':
                    name = player_data_parts[0][1:]; ip = player_data_parts[1] if len(player_data_parts) > 1 else ''; port = player_data_parts[2] if len(player_data_parts) > 2 else ''
                    color_id = int(player_data_parts[4]) if len(player_data_parts) > 4 else -1; faction_id = int(player_data_parts[5]) if len(player_data_parts) > 5 else -1
                    start_pos = int(player_data_parts[6]) if len(player_data_parts) > 6 else -1; team_id = (int(player_data_parts[7]) + 1) if len(player_data_parts) > 7 else 0
                    if index == 0: host_hex_ip = ip; host_port = port
                    players[player_num_current] = {'name': name, 'ip': ip, 'port': port, 'color': color_id, 'faction': faction_id, 'start_pos': start_pos, 'team': team_id, 'dc': disconnect_flags[index] if index < len(disconnect_flags) else 0, 'random': 0, 'is_ai': False}
                    player_nicks.append(name)
                    if faction_id == -2: observer_num_list.append(player_num_current)
                    else: player_num_list.append(player_num_current); teams.setdefault(team_id, []).append(player_num_current)
                elif player_type == 'C':
                    computer_player_in_game = True; difficulty_char = player_data_parts[0][1:]; name = comp_name(difficulty_char)
                    color_id = int(player_data_parts[1]) if len(player_data_parts) > 1 else -1; faction_id = int(player_data_parts[2]) if len(player_data_parts) > 2 else -1
                    start_pos = int(player_data_parts[3]) if len(player_data_parts) > 3 else -1; team_id = (int(player_data_parts[4]) + 1) if len(player_data_parts) > 4 else 0
                    players[player_num_current] = {'name': name, 'ip': '', 'port': '', 'color': color_id, 'faction': faction_id, 'start_pos': start_pos, 'team': team_id, 'dc': disconnect_flags[index] if index < len(disconnect_flags) else 0, 'random': 0, 'is_ai': True}
                    player_nicks.append(name); player_num_list.append(player_num_current); teams.setdefault(team_id, []).append(player_num_current)
                else: continue # Skip unknown types
                if players[player_num_current]['faction'] == -1: players[player_num_current]['random'] = 1
                pl_count += 1
            except (IndexError, ValueError) as e:
                print(f"  Warning: Error parsing slot {index} ('{player_raw}'): {e}")
                if player_num_current in players: del players[player_num_current]
                continue

        # --- Resolve Random Factions/Colors ---
        game_prng = prng.RandomGenerator(game_sd); total_colors = 8; total_factions = 12
        taken_colors = [False] * total_colors
        for p_data in players.values():
            if p_data['color'] != -1 and 0 <= p_data['color'] < total_colors: taken_colors[p_data['color']] = True
        for p_num in sorted(players.keys()):
            p_data = players[p_num]
            if p_data['faction'] == -1: p_data['faction'] = assign_random_faction(game_prng, total_factions, game_sd)
            if p_data['color'] == -1: p_data['color'] = assign_random_color(game_prng, total_colors, taken_colors)

        # --- Finalize Teams and Match Type ---
        teams, players = fix_teams(teams, players); match_type = get_match_type(teams)

        # --- Analyze Quit/End Game Data ---
        quit_data = {}
        for match in re.finditer(r'450400000(.)000000010201', hex_data):
            try: match_str_num = int(match.group(1), 16); match_index = match.start()
            except ValueError: continue
            if match_str_num in players: quit_data.setdefault(match_str_num, []).append(match_index)
        for p_num in quit_data: quit_data[p_num].sort()

        last_crc_data = {}; last_crc_index = -1; last_crc_frame_hex = "00000000"
        owner_crc_pattern = f"470400000{num_player:x}0000000200010201"
        last_crc_index = hex_data.rfind(owner_crc_pattern)
        if last_crc_index != -1:
            last_crc_frame_hex = hex_data[last_crc_index-8:last_crc_index]
            frame_crc_pattern = f"{last_crc_frame_hex}470400000(.)0000000200010201"
            for match in re.finditer(frame_crc_pattern, hex_data):
                 try: match_str_num = int(match.group(1), 16)
                 except ValueError: continue
                 if match_str_num in players: last_crc_data[match_str_num] = match.start()

        # --- Determine Player Status (Surrender, Exit, Idle) ---
        players_quit_frames = {p: {'surrender': 0, 'exit': 0, 'last_crc': '', 'surrender/exit?': 0, 'idle/kicked?': 0} for p in players}
        teams_data = {t: [quit_data.get(p, [-1])[0] for p in pl] for t, pl in teams.items()}
        found_winner, winning_team = find_winning_team(teams_data)
        update_players_data(num_player, hex_data, quit_data, teams, teams_data, winning_team, players_quit_frames, observer_num_list, last_crc_data, last_crc_index, found_winner)

        # --- Determine Actual Replay End Frame ---
        actual_replay_end_frame = rep_duration_header
        if found_winner and len(teams) > 1:
            try:
                loser_quit_indices = [max(quit_data[p_num]) for t, p_list in teams.items() if t != winning_team for p_num in p_list if p_num in quit_data]
                if loser_quit_indices: actual_replay_end_frame = extract_frame(hex_data, max(loser_quit_indices))
            except (ValueError, KeyError): pass
        elif num_player in quit_data: actual_replay_end_frame = extract_frame(hex_data, max(quit_data[num_player]))
        elif last_crc_index > 0: actual_replay_end_frame = hex_to_decimal(last_crc_frame_hex)
        if actual_replay_end_frame > rep_duration_header and rep_duration_header > 0: actual_replay_end_frame = rep_duration_header

        # --- Check for Idle/Kick (Second Pass - Optional/Simplified) ---
        # This section can be slow and complex. Consider simplifying or removing if not essential.
        update_players_data_again = False; idle_kick_indices = []
        player_final_message_frame = actual_replay_end_frame
        if player_final_message_frame >= 5400: # Idle check heuristic
            patterns_to_ignore = ["001b0000000", "00470400000", "00490400000", "00eb0300000", "00e90300000", "00220400000", "00450400000", "00f80300000", "00f90300000", "00fa0300000", "00fb0300000", "00fc0300000", "00fd0300000", "00fe0300000", "00ff0300000", "00000400000", "00010400000"]
            min_idle_diff = 900; min_kick_diff = 1800
            search_start_index = max(0, len(hex_data) - 200000) # Limit search range

            for p_num, p_status in players_quit_frames.items():
                if p_num in observer_num_list or p_status['surrender'] != 0: continue
                last_msg_index = -1
                try:
                    # Find last relevant message index (can be slow)
                    # Consider optimizing this search if it's a bottleneck
                    relevant_indices = [search_start_index + match.start()
                                        for match in re.finditer(f'00....00000{p_num:x}000000', hex_data[search_start_index:])
                                        if not any(match.group(0)[:11] == pattern for pattern in patterns_to_ignore)]
                    if relevant_indices:
                        last_msg_index = relevant_indices[-1]
                except re.error as re_err:
                    print(f"  Warning: Regex error during idle check for player {p_num}: {re_err}")
                    continue

                if last_msg_index != -1:
                    msg_frame = extract_frame(hex_data, last_msg_index + 2)
                    frame_diff = player_final_message_frame - msg_frame
                    if frame_diff >= min_idle_diff:
                         is_potential_kick = False
                         if p_status['exit'] != 0 and (p_status['exit'] - msg_frame) >= min_kick_diff: is_potential_kick = True
                         elif p_status['surrender/exit?'] != 0 and (p_status['surrender/exit?'] - msg_frame) >= min_kick_diff: is_potential_kick = True
                         elif p_num not in quit_data and frame_diff >= min_kick_diff: is_potential_kick = True
                         if is_potential_kick:
                              p_status['idle/kicked?'] = msg_frame
                              if p_num not in quit_data: quit_data[p_num] = []
                              quit_data[p_num].insert(0, last_msg_index + 2); quit_data[p_num].sort()
                              idle_kick_indices.append(last_msg_index + 2); update_players_data_again = True

        if update_players_data_again:
            teams_data = {t: [quit_data.get(p, [-1])[0] for p in pl] for t, pl in teams.items()}
            found_winner, winning_team = find_winning_team(teams_data)
            update_players_data(num_player, hex_data, quit_data, teams, teams_data, winning_team, players_quit_frames, observer_num_list, last_crc_data, last_crc_index, found_winner)

        # --- Determine Final Match Result ---
        match_result = 'Unknown'; winning_team_string = 'Unknown'
        if desync == 1: match_result = 'Desync'; winning_team_string = 'No Result (Desync)'; found_winner = False
        elif computer_player_in_game: match_result = 'AI Player Present'; winning_team_string = 'No Result (AI Player)'; found_winner = False
        elif len(teams) <= 1 and not computer_player_in_game: match_result = 'No Opponents'; winning_team_string = 'No Result (No Opponents)'; found_winner = False
        elif found_winner:
            winning_team_string = str(winning_team)
            if num_player in player_num_list: match_result = 'Win' if players[num_player]['team'] == winning_team else 'Loss'
            elif num_player in observer_num_list: match_result = f'Observed Team {winning_team} Win'
            else: match_result = f'Team {winning_team} Won (Owner Unknown)'
        else: # No winner found
            if last_crc_index == -1 and start_time > 0: match_result = 'DC at Start'; winning_team_string = 'No Result (DC at Start)'
            elif num_player in quit_data: match_result = 'Quit/Exit (No Winner)'; winning_team_string = 'No Result (Inconclusive)'
            else: match_result = 'Inconclusive / Draw?'; winning_team_string = 'No Result (Inconclusive)'

        # --- Calculate Placement ---
        placement = {}
        if found_winner and len(teams) > 1:
            try:
                other_teams_quit_times = []
                for team_id, p_list in teams.items():
                    if team_id != winning_team:
                         team_times = [max(p_s['exit'], p_s['surrender'], p_s['idle/kicked?'], p_s['surrender/exit?']) for p_n in p_list if p_n in players_quit_frames for p_s in [players_quit_frames[p_n]]]
                         other_teams_quit_times.append((team_id, max(team_times) if team_times else 0))
                other_teams_quit_times.sort(key=lambda x: x[1], reverse=True)
                placement = {winning_team: ordinal(1)}
                for rank, (team_id, _) in enumerate(other_teams_quit_times, start=2): placement[team_id] = ordinal(rank)
            except Exception as e: print(f"  Warning: Error calculating placement: {e}"); placement = {}

        # --- Compile Results ---
        exe_check = "Success" if exe_crc == 3660270360 else "Failed"; ini_check = "Success" if ini_crc == 4272612339 else "Failed"
        replay_info_list = [
            ("Version String", version), ("Build Date", build_date), ("EXE check (1.04)", exe_check), ("INI check (1.04)", ini_check),
            ("Map Name", map_name), ("Start Cash", start_cash), ("SW Restriction", sw_restriction), ("Match Type", match_type),
            ("Replay Duration", ddhhmmss(actual_replay_end_frame / 30.0)), ("Match Mode", get_match_mode(host_hex_ip, host_port)),
            ("Match Result", match_result), ("Winning Team", winning_team_string),
        ]
        if num_player in players:
             replay_info_list.extend([("Player Name (Owner)", players[num_player]["name"]), ('Color (Owner)', colors.get(players[num_player]['color'], 'Unknown')), ('Faction (Owner)', factions.get(players[num_player]['faction'], 'Unknown'))])
        else: replay_info_list.append(("Player Name (Owner)", f"Unknown (Num: {num_player})"))
        replay_info_list.append(("Game Seed", game_sd)); replay_info_list.append(("Raw Begin Timestamp", start_time))
        replay_info_list.append(("Raw Duration Frames", actual_replay_end_frame)); replay_info_list.append(("Raw Header Duration", rep_duration_header))
        replay_info_list.append(("Raw Match Data String", match_data_str))
        if mode == 2 and file_path: replay_info_list.insert(0, ("Match ID", get_match_id(file_path, start_time, game_sd, match_type, map_crc, player_nicks)))

        player_infos_list = []
        for p_num, p_data in players.items():
            p_status = players_quit_frames[p_num]; player_placement = placement.get(p_data['team'], '')
            player_infos_list.append((
                p_data['team'], p_data['ip'], p_data['name'], f"{factions.get(p_data['faction'], 'Unknown')} {'(R)' if p_data['random']==1 else ''}",
                ddhhmmss(p_status['surrender/exit?'] / 30.0), ddhhmmss(p_status['surrender'] / 30.0), ddhhmmss(p_status['exit'] / 30.0),
                ddhhmmss(p_status['idle/kicked?'] / 30.0), p_status['last_crc'], player_placement, colors.get(p_data['color'], 'Unknown'),
                p_data['is_ai'], p_num
            ))

        # --- Return based on rename_info flag ---
        if rename_info:
            teams_filename = []
            for team_id, p_list in sorted(teams.items()):
                team_str = " ".join(f"{players[p_num]['name']}({rename_factions.get(players[p_num]['faction'], 'unk')})" for p_num in p_list)
                teams_filename.append(team_str)
            teams_part = ' vs '.join(teams_filename); date_str = datetime.fromtimestamp(start_time, UTC).strftime('%y%m%d'); map_part = sanitize_filename(map_name)
            filename = f"{match_type} [{date_str}] ({map_part}) {teams_part}"
            return sanitize_filename(filename)
        else:
            return replay_info_list, player_infos_list

    except Exception as e:
        print(f"--- CRITICAL ERROR processing {file_path}: {e} ---")
        import traceback; traceback.print_exc()
        return None if not rename_info else "critical_parsing_error"


# --- Minimal Parser for Pass 1 ---

def parse_minimal_header_for_key(filename):
    """Reads only essential header parts for unique match identification."""
    try:
        with open(filename, 'rb') as f:
            magic = f.read(6);
            if magic != b'GENREP': return None
            begin_timestamp, _, replay_duration = struct.unpack('<III', f.read(12))
            f.seek(10, 1); _ = read_null_terminated_string(f, encoding='utf-16'); f.seek(16, 1)
            version_str = read_null_terminated_string(f, encoding='utf-16') # Read version
            _ = read_null_terminated_string(f, encoding='utf-16'); f.seek(12, 1)
            match_data_str = read_null_terminated_string(f);
            if not match_data_str: return None

            # --- Version Check (Case-Insensitive) ---
            if not version_str or version_str.lower() not in VALID_VERSIONS_LOWER:
                return {'invalid_version': True} # Indicate invalid version

            sd_match = re.search(r';SD=(\d+);', match_data_str); map_match = re.search(r';M=([^;]+);', match_data_str); slot_match = re.search(r';S=([^;]+);', match_data_str)
            if not sd_match or not map_match or not slot_match: return None

            game_sd = int(sd_match.group(1)); map_full_path = map_match.group(1); map_name = map_full_path[map_full_path.rfind('/')+1:] if '/' in map_full_path else map_full_path
            if map_name == "Unknown Map": return None

            slots_part = slot_match.group(1); slots_data = re.split(r':(?=[HCXO])', slots_part)
            players_for_hash = []; has_unknown_faction = False; has_ai = False

            for slot in slots_data:
                if not slot or slot[0] in ('X', 'O'): continue
                parts = slot.split(','); player_type = slot[0]; name = parts[0][1:]; faction_index = -99
                try:
                    if player_type == 'H':
                        if len(parts) > 5: faction_index = int(parts[5])
                    elif player_type == 'C':
                        has_ai = True
                        if len(parts) > 2: faction_index = int(parts[2])
                    is_observer = (player_type == 'H' and faction_index == -2)
                    if not is_observer:
                        player_name = f"AI_{name}" if player_type == 'C' else name
                        if faction_index < -2 or faction_index > 11: has_unknown_faction = True; break
                        players_for_hash.append(f"{player_name}|{faction_index}")
                except (ValueError, IndexError): has_unknown_faction = True; break
            if has_unknown_faction: return {'unknown_faction': True}

            players_for_hash.sort(); player_hash = hashlib.md5(";".join(players_for_hash).encode('utf-8')).hexdigest()

            return {
                'game_sd': game_sd, 'map_name': map_name, 'begin_timestamp': begin_timestamp,
                'duration': replay_duration, 'player_hash': player_hash,
                'unknown_faction': False, 'has_ai': has_ai, 'invalid_version': False
            }
    except FileNotFoundError: return None
    except Exception: return None

# --- Database Functions ---

def setup_database():
    """Creates the database and table including has_ai column."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS unique_matches (
            match_key TEXT PRIMARY KEY,
            longest_replay_path TEXT NOT NULL,
            max_duration INTEGER NOT NULL,
            game_seed INTEGER,
            map_name TEXT,
            match_timestamp INTEGER,
            player_hash TEXT,
            has_ai INTEGER DEFAULT 0
        )
    ''')
    try: cursor.execute("ALTER TABLE unique_matches ADD COLUMN has_ai INTEGER DEFAULT 0")
    except sqlite3.OperationalError: pass # Column likely already exists
    conn.commit()
    conn.close()

def generate_match_key(game_sd, map_name, begin_timestamp, player_hash):
    """Generates the unique match key."""
    rounded_ts = round(begin_timestamp / 60) * 60
    sanitized_map_name = re.sub(r'\s+', '_', map_name).lower() # Use lowercase map name in key
    return f"{game_sd}_{sanitized_map_name}_{rounded_ts}_{player_hash}"


# --- Worker Function for Pass 2 ---

def process_single_replay_worker(rep_file):
    """Parses a single replay fully and returns structured results for aggregation."""
    try:
        parsed_data = get_replay_info(rep_file, mode=1)
        if parsed_data is None: return {'status': 'error', 'file': rep_file, 'reason': 'get_replay_info_failed'}
        replay_info_list, player_infos = parsed_data

        has_unknown_faction = False; player_factions_cleaned = []
        for player_info in player_infos:
            faction_str = player_info[3]; faction_or_observer = faction_str.replace(' (R)', '').strip()
            player_factions_cleaned.append(faction_or_observer)
            if faction_or_observer == "Unknown": has_unknown_faction = True; break
        if has_unknown_faction: return {'status': 'error', 'file': rep_file, 'reason': 'unknown_faction_in_pass2'}

        winning_team_value = next((v for k, v in replay_info_list if k == "Winning Team"), "Unknown")
        match_type = next((v for k, v in replay_info_list if k == "Match Type"), "Unknown")
        map_name = next((v for k, v in replay_info_list if k == "Map Name"), "Unknown Map")
        is_ai_present = any(p[11] for p in player_infos)

        if is_ai_present: return {'status': 'no_winner', 'file': rep_file, 'reason': 'AI Player Present', 'ai': True}
        if match_type == "Unknown": return {'status': 'no_winner', 'file': rep_file, 'reason': 'unknown_match_type', 'ai': is_ai_present}

        # Determine category (Case-Insensitive Map Check)
        category = None
        is_pro_map = False
        if match_type == "1v1" and map_name.lower() in ALLOWED_MAPS_LOWER:
            category = "1v1_Pro_Maps"
            is_pro_map = True

        result_data = {
            'status': 'ok', 'file': rep_file, 'match_type': match_type, 'category': category, 'is_pro_map': is_pro_map,
            'is_winner': isinstance(winning_team_value, str) and winning_team_value.isdigit(),
            'winning_team': int(winning_team_value) if isinstance(winning_team_value, str) and winning_team_value.isdigit() else None,
            'ai': is_ai_present,
            'replay_detail': None, 'faction_stats': {}, 'matchup_stats': None
        }

        if result_data['is_winner']:
            replay_detail = {"file": rep_file, "players": []}; player_index = 0
            for player_info in player_infos:
                name = player_info[2]; placement = player_info[9]; faction_or_observer = player_factions_cleaned[player_index]
                if faction_or_observer != "Observer": replay_detail["players"].append((name, faction_or_observer, placement))
                player_index += 1
            result_data['replay_detail'] = replay_detail

            player_index = 0; team_to_faction_map = {}
            for player_info in player_infos:
                team = player_info[0]; faction_or_observer = player_factions_cleaned[player_index]; player_index += 1
                if faction_or_observer != "Observer":
                    faction = faction_or_observer
                    result_data['faction_stats'].setdefault(faction, {'wins': 0, 'games_played': 0})
                    result_data['faction_stats'][faction]['games_played'] += 1
                    if team == result_data['winning_team']: result_data['faction_stats'][faction]['wins'] += 1
                    if match_type == "1v1": team_to_faction_map[team] = faction

            if match_type == "1v1" and len(team_to_faction_map) == 2:
                factions_list = sorted(team_to_faction_map.values()); faction1, faction2 = factions_list[0], factions_list[1]
                if faction1 != faction2:
                    key = (faction1, faction2); winning_faction = team_to_faction_map.get(result_data['winning_team'])
                    if winning_faction:
                        f1_win = 1 if winning_faction == faction1 else 0; f2_win = 1 if winning_faction == faction2 else 0
                        result_data['matchup_stats'] = {'key': key, 'f1_win': f1_win, 'f2_win': f2_win}
        else:
             result_data['status'] = 'no_winner'; result_data['reason'] = winning_team_value

        return result_data
    except Exception as e:
        return {'status': 'error', 'file': rep_file, 'reason': f"worker_exception: {e}"}


# --- Main Execution ---
if __name__ == "__main__":
    start_time_script = time.time()
    files_to_delete = set()

    try:
        if os.path.exists(DB_FILE): print(f"Removing existing database: {DB_FILE}"); os.remove(DB_FILE)
    except OSError as e: print(f"Warning: Could not remove existing database {DB_FILE}: {e}")
    setup_database()
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    print("--- Pass 1: Scanning replays and identifying unique matches (Optimized) ---")
    rep_files = glob.glob('**/*.rep', recursive=True)
    if not rep_files: print("No .rep files found. Exiting."); exit()

    processed_count_pass1 = 0; skipped_parsing_errors = 0; skipped_unknown_faction_pass1 = 0
    skipped_invalid_version = 0; total_duplicates_skipped = 0
    start_time_pass1 = time.time()

    for rep_file in rep_files:
        processed_count_pass1 += 1
        if processed_count_pass1 % 500 == 0:
             elapsed = time.time() - start_time_pass1; rate = processed_count_pass1 / elapsed if elapsed > 0 else 0
             print(f"\r  Pass 1: Processed {processed_count_pass1}/{len(rep_files)} files ({rate:.1f} files/sec). Dup: {total_duplicates_skipped}, Err: {skipped_parsing_errors}, UnkF: {skipped_unknown_faction_pass1}, InvV: {skipped_invalid_version}...", end="")

        key_info = parse_minimal_header_for_key(rep_file)

        if key_info is None: skipped_parsing_errors += 1; continue
        if key_info.get('invalid_version', False): skipped_invalid_version += 1; continue # Skip invalid version, DO NOT delete
        if key_info.get('unknown_faction', False): skipped_unknown_faction_pass1 += 1; files_to_delete.add(rep_file); continue # Mark unknown faction for deletion

        game_sd = key_info['game_sd']; map_name = key_info['map_name']; begin_timestamp = key_info['begin_timestamp']
        replay_duration = key_info['duration']; player_hash = key_info['player_hash']; has_ai = key_info['has_ai']

        match_key = generate_match_key(game_sd, map_name, begin_timestamp, player_hash)

        cursor.execute("SELECT max_duration, longest_replay_path FROM unique_matches WHERE match_key = ?", (match_key,))
        result = cursor.fetchone()

        if result:
            stored_duration, stored_path = result
            if replay_duration > stored_duration:
                files_to_delete.add(stored_path); cursor.execute("UPDATE unique_matches SET longest_replay_path = ?, max_duration = ?, has_ai = ? WHERE match_key = ?", (rep_file, replay_duration, has_ai, match_key))
            else:
                files_to_delete.add(rep_file); total_duplicates_skipped += 1
        else:
            cursor.execute("INSERT INTO unique_matches VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (match_key, rep_file, replay_duration, game_sd, map_name, begin_timestamp, player_hash, has_ai))

        if processed_count_pass1 % 2000 == 0: conn.commit()

    conn.commit()
    end_time_pass1 = time.time()
    print(f"\r{' ' * 120}\r--- Pass 1 Complete ({end_time_pass1 - start_time_pass1:.2f} seconds) ---")

    cursor.execute("SELECT longest_replay_path FROM unique_matches WHERE has_ai = 1")
    ai_game_paths = [row[0] for row in cursor.fetchall()]; ai_games_to_delete_count = len(ai_game_paths)
    files_to_delete.update(ai_game_paths); print(f"  Marked {ai_games_to_delete_count} unique AI games for deletion.")

    cursor.execute("SELECT longest_replay_path FROM unique_matches WHERE has_ai = 0")
    unique_replay_files_for_stats = [row[0] for row in cursor.fetchall()]
    conn.close()

    print(f"  Total files scanned: {processed_count_pass1}")
    print(f"  Unique non-AI matches identified for stats: {len(unique_replay_files_for_stats)}")
    print(f"  Duplicate replays marked for deletion: {total_duplicates_skipped}")
    print(f"  Skipped (parsing errors): {skipped_parsing_errors}")
    print(f"  Skipped (invalid version): {skipped_invalid_version}")
    print(f"  Unknown faction replays marked for deletion: {skipped_unknown_faction_pass1}")


    print("\n--- Pass 2: Processing unique non-AI replays for statistics (Parallelized) ---")
    match_type_data = {} # Holds aggregated data for different categories
    total_valid_winners = 0; total_no_winners_pass2 = 0; errors_pass2 = 0; processed_unique_count = 0

    if not unique_replay_files_for_stats:
         print("No unique non-AI replays found to process in Pass 2.")
    else:
        num_workers = max(1, cpu_count() - 1)
        print(f"  Processing {len(unique_replay_files_for_stats)} unique matches using {num_workers} workers.")
        start_time_pass2 = time.time()

        with Pool(processes=num_workers) as pool:
            results_iterator = pool.imap_unordered(process_single_replay_worker, unique_replay_files_for_stats)
            for i, result in enumerate(results_iterator):
                processed_unique_count += 1
                if processed_unique_count % 100 == 0 or processed_unique_count == len(unique_replay_files_for_stats):
                     elapsed = time.time() - start_time_pass2; rate = processed_unique_count / elapsed if elapsed > 0 else 0
                     print(f"\r  Pass 2: Processed {processed_unique_count}/{len(unique_replay_files_for_stats)} unique replays ({rate:.1f} replays/sec)...", end="")

                if not result: errors_pass2 += 1; continue
                if result['status'] == 'error': errors_pass2 += 1; total_no_winners_pass2 += 1; continue
                if result['status'] == 'no_winner': total_no_winners_pass2 += 1; continue
                if result['status'] == 'ok':
                    primary_mt = result['match_type']; category = result.get('category')
                    keys_to_update = [primary_mt]
                    if category: keys_to_update.append(category)

                    is_winner_replay = result['is_winner'] # Check if this replay had a winner

                    for mt_key in keys_to_update:
                        if mt_key not in match_type_data:
                            match_type_data[mt_key] = {'factions': {}, 'replay_details': [], 'matchups': {} if mt_key.startswith("1v1") else None}
                        if mt_key.startswith("1v1") and (match_type_data[mt_key]['matchups'] is None): match_type_data[mt_key]['matchups'] = {}

                        if is_winner_replay:
                            if mt_key == primary_mt and result['replay_detail']: match_type_data[mt_key]['replay_details'].append(result['replay_detail'])
                            for faction, stats in result['faction_stats'].items():
                                match_type_data[mt_key]['factions'].setdefault(faction, {'wins': 0, 'games_played': 0})
                                match_type_data[mt_key]['factions'][faction]['wins'] += stats['wins']
                                match_type_data[mt_key]['factions'][faction]['games_played'] += stats['games_played']
                            if mt_key.startswith("1v1") and result['matchup_stats']:
                                key = result['matchup_stats']['key']
                                if 'matchups' not in match_type_data[mt_key] or match_type_data[mt_key]['matchups'] is None: match_type_data[mt_key]['matchups'] = {}
                                match_type_data[mt_key]['matchups'].setdefault(key, {'faction1_wins': 0, 'faction2_wins': 0, 'replays': 0})
                                match_type_data[mt_key]['matchups'][key]['faction1_wins'] += result['matchup_stats']['f1_win']
                                match_type_data[mt_key]['matchups'][key]['faction2_wins'] += result['matchup_stats']['f2_win']
                                match_type_data[mt_key]['matchups'][key]['replays'] += 1

                    # Increment overall winner count only once per winning replay
                    if is_winner_replay:
                         # Check if this replay has already been counted towards total_valid_winners
                         # This requires tracking processed files or using a flag, complex with parallel
                         # Simple approach: Count based on the primary match type aggregation
                         if primary_mt in keys_to_update: # Should always be true here
                              # This might slightly overcount if a replay contributes to multiple categories
                              # A more accurate way is needed if perfect count is critical
                              pass # We'll sum valid winners at the end based on aggregated data
                    else:
                        total_no_winners_pass2 += 1

        end_time_pass2 = time.time()
        print(f"\r{' ' * 120}\r--- Pass 2 Complete ({end_time_pass2 - start_time_pass2:.2f} seconds) ---")
        print(f"  Worker errors during processing: {errors_pass2}")

        # Recalculate total valid winners based on aggregated data (more accurate with categories)
        total_valid_winners = sum(len(data.get('replay_details', [])) for mt, data in match_type_data.items() if not mt.endswith("_Pro_Maps"))


    print("\n--- Generating Charts ---")
    charts_created = []
    if MATPLOTLIB_AVAILABLE:
        try:
            output_order = sorted(match_type_data.keys(), key=lambda x: (x.split('_')[0], x))
            for mt_key in output_order:
                data = match_type_data[mt_key]
                is_pro_map_category = mt_key.endswith("_Pro_Maps")
                category_title_suffix = " (Pro Maps)" if is_pro_map_category else ""

                # Overall Win Rate Bar Chart
                faction_stats = data.get('factions', {})
                if faction_stats:
                    plot_data = [{'label': f, 'rate': s['wins'] / s['games_played']} for f, s in faction_stats.items() if s['games_played'] > 0]
                    if plot_data:
                        plot_data.sort(key=lambda x: x['label'])
                        labels = [item['label'] for item in plot_data]; win_rates = [item['rate'] for item in plot_data]
                        plt.style.use('seaborn-v0_8-darkgrid'); fig, ax = plt.subplots(figsize=(max(8, len(labels)*0.6), 6))
                        bars = ax.bar(labels, win_rates, color=plt.cm.Paired(np.linspace(0, 1, len(labels))))
                        ax.set_ylabel('Win Rate'); ax.set_title(f'Overall Faction Win Rates - {mt_key}{category_title_suffix}')
                        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0)); ax.set_ylim(0, max(1.0, max(win_rates) * 1.1 if win_rates else 1.0))
                        ax.bar_label(bars, fmt='{:,.1%}', padding=3, fontsize=9); plt.xticks(rotation=45, ha='right', fontsize=9); plt.tight_layout()
                        chart_filename = f"winrate_overall_{sanitize_filename(mt_key)}.png"; plt.savefig(chart_filename); plt.close(fig)
                        charts_created.append(chart_filename); print(f"  Created chart: {chart_filename}")

                # 1v1 Matchup Horizontal Stacked Bar Chart
                if mt_key.startswith("1v1"):
                    matchups_data = data.get('matchups')
                    if matchups_data:
                        plot_data_1v1 = []
                        for matchup_key, matchup_stats in matchups_data.items():
                            if matchup_stats['replays'] > 0:
                                faction1, faction2 = matchup_key; winrate_f1_pct = (matchup_stats['faction1_wins'] / matchup_stats['replays']) * 100.0; winrate_f2_pct = 100.0 - winrate_f1_pct
                                if winrate_f1_pct >= winrate_f2_pct: higher_f, lower_f, higher_r, lower_r = faction1, faction2, winrate_f1_pct, winrate_f2_pct
                                else: higher_f, lower_f, higher_r, lower_r = faction2, faction1, winrate_f2_pct, winrate_f1_pct
                                plot_data_1v1.append({'sort_key': matchup_key, 'label': f"{higher_f} vs {lower_f}", 'higher_rate': higher_r, 'lower_rate': lower_r})
                        if plot_data_1v1:
                            plot_data_1v1.sort(key=lambda x: x['sort_key'])
                            matchup_labels = [item['label'] for item in plot_data_1v1]; higher_rates_percent = [item['higher_rate'] for item in plot_data_1v1]; lower_rates_percent = [item['lower_rate'] for item in plot_data_1v1]
                            y = np.arange(len(matchup_labels)); bar_height = 0.6
                            plt.style.use('seaborn-v0_8-darkgrid'); fig, ax = plt.subplots(figsize=(10, max(6, len(matchup_labels) * 0.35)))
                            bars_higher = ax.barh(y, higher_rates_percent, height=bar_height, color='forestgreen', label='Higher Win Rate %')
                            bars_lower = ax.barh(y, lower_rates_percent, height=bar_height, left=higher_rates_percent, color='indianred', label='Lower Win Rate %')
                            ax.set_yticks(y); ax.set_yticklabels(matchup_labels, fontsize=9); ax.set_xlabel("Win Rate (%)"); ax.set_title(f"1v1 Matchup Win Rates - {mt_key}{category_title_suffix}")
                            ax.legend(title="Faction Performance"); ax.set_xlim(0, 100); ax.xaxis.set_major_formatter(mtick.PercentFormatter())
                            for i, bar_h in enumerate(bars_higher):
                                h_rate = higher_rates_percent[i]; l_rate = lower_rates_percent[i]
                                if h_rate > 12: ax.text(h_rate / 2, bar_h.get_y() + bar_h.get_height()/2, f"{h_rate:.1f}%", va='center', ha='center', color='white', fontsize=8, fontweight='bold')
                                elif h_rate > 0.1: ax.text(h_rate + 1, bar_h.get_y() + bar_h.get_height()/2, f"{h_rate:.1f}%", va='center', ha='left', color='black', fontsize=7)
                                if l_rate > 12: ax.text(h_rate + (l_rate / 2), bar_h.get_y() + bar_h.get_height()/2, f"{l_rate:.1f}%", va='center', ha='center', color='white', fontsize=8, fontweight='bold')
                                elif l_rate > 0.1: ax.text(h_rate + l_rate + 1, bar_h.get_y() + bar_h.get_height()/2, f"{l_rate:.1f}%", va='center', ha='left', color='black', fontsize=7)
                            plt.tight_layout(pad=1.5)
                            chart_filename = f"winrate_matchups_{sanitize_filename(mt_key)}_stacked_h.png"; plt.savefig(chart_filename); plt.close(fig)
                            charts_created.append(chart_filename); print(f"  Created chart: {chart_filename}")
        except Exception as e:
            print(f"\n--- Error during Chart Generation: {e} ---")
            # import traceback; traceback.print_exc()
    else:
        print("--- Chart Generation Skipped (matplotlib or numpy not available) ---")


    print("\n--- Writing results to win_rates.txt ---")
    with open("win_rates.txt", "w", encoding='utf-8') as f:
        output_order = sorted(match_type_data.keys(), key=lambda x: (x.split('_')[0], x))
        for mt_key in output_order:
            data = match_type_data[mt_key]
            is_pro_map_category = mt_key.endswith("_Pro_Maps")
            category_title_suffix = " (Pro Maps)" if is_pro_map_category else ""
            category_note = " (Pro Maps Only)" if is_pro_map_category else ""
            replay_list_note = " (Pro Maps Only, Non-AI)" if is_pro_map_category else " (Non-AI)"

            f.write(f"--- Match Type: {mt_key}{category_title_suffix} ---\n\n")

            if not is_pro_map_category: # Only print replay list for primary categories
                f.write(f"Replays with Winners (Unique Longest{replay_list_note}):\n")
                details_list = data.get('replay_details', [])
                if not details_list: f.write("  (No valid replays recorded for this type)\n")
                else:
                    details_list.sort(key=lambda x: x['file'])
                    for replay in details_list:
                        f.write(f"  Replay: {replay['file']}\n")
                        if not replay['players']: f.write("    (No player data found)\n")
                        else:
                            replay['players'].sort(key=lambda p: (p[2] if p[2] else 'Z', p[0]))
                            for player in replay["players"]: name, faction, placement = player; f.write(f"    {name:<25} {faction:<20} {placement}\n")
                        f.write("\n")
                f.write("\n")

            if mt_key.startswith("1v1"):
                f.write(f"Matchup Win Rates{category_note} (excluding mirrors):\n")
                matchups = data.get('matchups')
                if not matchups: f.write("  (No non-mirror 1v1 matchups found)\n")
                else:
                    for key, matchup_stats in sorted(matchups.items()):
                        faction1, faction2 = key; total_matchup_replays = matchup_stats['replays']
                        if total_matchup_replays > 0:
                            win_rate_f1 = matchup_stats['faction1_wins'] / total_matchup_replays; win_rate_f2 = matchup_stats['faction2_wins'] / total_matchup_replays
                            if win_rate_f1 > win_rate_f2: f.write(f"  {faction1:<20} vs {faction2:<20}: {faction1} wins {win_rate_f1:>7.2%} ({total_matchup_replays} replays)\n")
                            elif win_rate_f2 > win_rate_f1: f.write(f"  {faction2:<20} vs {faction1:<20}: {faction2} wins {win_rate_f2:>7.2%} ({total_matchup_replays} replays)\n")
                            else: f.write(f"  {faction1:<20} vs {faction2:<20}: 50.00% win rate ({total_matchup_replays} replays)\n")
                f.write("\n")

            f.write(f"Overall Win Rates{category_note} (Player-Based, Non-AI):\n")
            faction_stats = data.get('factions', {})
            if not faction_stats: f.write("  (No faction data available for win rate calculation)\n")
            else:
                sorted_factions = sorted(faction_stats.items(), key=lambda item: item[0])
                for faction, stats in sorted_factions:
                    wins = stats['wins']; games_played = stats['games_played']
                    if games_played > 0: win_rate = wins / games_played; f.write(f"  {faction:<20}: {win_rate:>7.2%} ({wins} wins / {games_played} games played)\n")
                    else: f.write(f"  {faction:<20}: No games played\n")
            f.write("\n" * 2)

        f.write("--- Summary ---\n")
        unique_matches_count = len(unique_replay_files_for_stats) + ai_games_to_delete_count if 'unique_replay_files_for_stats' in locals() else 0
        f.write(f"Total replay files scanned: {processed_count_pass1}\n")
        f.write(f"Unique matches identified (including AI): {unique_matches_count}\n")
        f.write(f"Duplicate replays marked for deletion: {total_duplicates_skipped}\n")
        f.write(f"Skipped during scan (parsing errors): {skipped_parsing_errors}\n")
        f.write(f"Skipped during scan (invalid version): {skipped_invalid_version}\n")
        f.write(f"Unknown faction replays marked for deletion: {skipped_unknown_faction_pass1}\n")
        f.write(f"Unique AI replays marked for deletion: {ai_games_to_delete_count}\n")
        f.write(f"Unique non-AI matches processed for stats: {processed_unique_count}\n")
        f.write(f"Errors during unique non-AI replay processing: {errors_pass2}\n")
        f.write(f"Unique non-AI matches with valid winners (used for stats): {total_valid_winners}\n")
        f.write(f"Unique non-AI matches without valid winners (Not listed): {total_no_winners_pass2}\n")
        if charts_created:
             f.write("\nCharts Generated:\n"); [f.write(f"  - {chart}\n") for chart in charts_created]


    print(f"\n--- Deleting Marked Replays ({len(files_to_delete)} files) ---")
    deleted_count = 0; error_delete_count = 0
    if not files_to_delete: print("  No files marked for deletion.")
    else:
        for file_to_del in files_to_delete:
            try:
                os.remove(file_to_del); deleted_count += 1
                if deleted_count % 100 == 0: print(f"\r  Deleted {deleted_count}/{len(files_to_delete)} files...", end="")
            except FileNotFoundError: error_delete_count += 1
            except OSError as e: print(f"  Error deleting file {file_to_del}: {e}"); error_delete_count += 1
        print(f"\r{' ' * 50}\r  Finished deleting. Deleted: {deleted_count}, Errors: {error_delete_count}")


    print(f"\nResults written to win_rates.txt")
    if charts_created: print("Charts generated:"); [print(f"  - {chart}") for chart in charts_created]
    print(f"Based on {total_valid_winners} unique non-AI matches with valid winners.")
    print(f"Total execution time: {time.time() - start_time_script:.2f} seconds")

    try:
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
    except OSError as e: print(f"Warning: Could not remove database file {DB_FILE}: {e}")