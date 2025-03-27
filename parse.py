import os
import struct
import json
import glob

# ----------------------------
# Custom RNG Replication (C++ ADC-based)
# ----------------------------
GAME_LOGIC_SEED = [
    0xf22d0e56, 0x883126e9, 0xc624dd2f,
    0x702c49c, 0x9e353f7d, 0x6fdf3b64
]

def seed_random(seed, seed_array):
    seed_array[0] = (seed + 0xf22d0e56) & 0xFFFFFFFF
    seed_array[1] = (seed_array[0] + (0x883126e9 - 0xf22d0e56)) & 0xFFFFFFFF
    seed_array[2] = (seed_array[1] + (0xc624dd2f - 0x883126e9)) & 0xFFFFFFFF
    seed_array[3] = (seed_array[2] + (0x0702c49c - 0xc624dd2f)) & 0xFFFFFFFF
    seed_array[4] = (seed_array[3] + (0x9e353f7d - 0x0702c49c)) & 0xFFFFFFFF
    seed_array[5] = (seed_array[4] + (0x6fdf3b64 - 0x9e353f7d)) & 0xFFFFFFFF

def init_random(seed):
    global GAME_LOGIC_SEED
    seed_random(seed, GAME_LOGIC_SEED)

def adc(a, b, carry):
    s = a + b + carry
    sum_val = s & 0xFFFFFFFF
    new_carry = s >> 32
    return sum_val, new_carry

def random_value(seed_array):
    carry = 0
    ax, carry = adc(seed_array[5], seed_array[4], carry)
    seed_array[4] = ax

    ax, carry = adc(ax, seed_array[3], carry)
    seed_array[3] = ax

    ax, carry = adc(ax, seed_array[2], carry)
    seed_array[2] = ax

    ax, carry = adc(ax, seed_array[1], carry)
    seed_array[1] = ax

    ax, carry = adc(ax, seed_array[0], carry)
    seed_array[0] = ax

    seed_array[5] = (seed_array[5] + 1) & 0xFFFFFFFF
    if seed_array[5] == 0:
        seed_array[4] = (seed_array[4] + 1) & 0xFFFFFFFF
        if seed_array[4] == 0:
            seed_array[3] = (seed_array[3] + 1) & 0xFFFFFFFF
            if seed_array[3] == 0:
                seed_array[2] = (seed_array[2] + 1) & 0xFFFFFFFF
                if seed_array[2] == 0:
                    seed_array[1] = (seed_array[1] + 1) & 0xFFFFFFFF
                    if seed_array[1] == 0:
                        seed_array[0] = (seed_array[0] + 1) & 0xFFFFFFFF
                        ax = (ax + 1) & 0xFFFFFFFF
    return ax

def get_game_logic_random_value(lo, hi):
    delta = hi - lo + 1
    if delta == 0:
        return hi
    return (random_value(GAME_LOGIC_SEED) % delta) + lo

# ----------------------------
# Template and Color Setup (updated names)
# ----------------------------
AVAILABLE_TEMPLATES = {
    1: "Observer",
    2: "USA",
    3: "China",
    4: "GLA",
    5: "USA: Super Weapon",
    6: "USA: Laser",
    7: "USA: Air Force",
    8: "China: Tank",
    9: "China: Infantry",
    10: "China: Nuke",
    11: "GLA: Toxin",
    12: "GLA: Demolition",
    13: "GLA: Stealth"
}

AVAILABLE_COLORS = {
    0: "Yellow",
    1: "Red",
    2: "Blue",
    3: "Green",
    4: "Orange",
    5: "Cyan",
    6: "Purple",
    7: "Pink"
}

# ----------------------------
# Replay File Parsing
# ----------------------------
MAX_SLOTS = 8

ARG_TYPE_MAP = {
    0: ('i', 4),
    1: ('f', 4),
    2: ('?', 1),
    3: ('I', 4),
    4: ('I', 4),
    5: ('I', 4),
    6: ('fff', 12),
    7: ('ii', 8),
    8: ('iiii', 16),
    9: ('I', 4),
    10: ('H', 2),
}

MESSAGE_TYPE_MAP = {
    1000: "MSG_BEGIN_NETWORK_MESSAGES",
    1001: "MSG_CREATE_SELECTED_GROUP",
    1002: "MSG_CREATE_SELECTED_GROUP_NO_SOUND",
    1003: "MSG_DESTROY_SELECTED_GROUP",
    1004: "MSG_REMOVE_FROM_SELECTED_GROUP",
    1005: "MSG_SELECTED_GROUP_COMMAND",
    1006: "MSG_CREATE_TEAM0",
    1007: "MSG_CREATE_TEAM1",
    1008: "MSG_CREATE_TEAM2",
    1009: "MSG_CREATE_TEAM3",
    1010: "MSG_CREATE_TEAM4",
    1011: "MSG_CREATE_TEAM5",
    1012: "MSG_CREATE_TEAM6",
    1013: "MSG_CREATE_TEAM7",
    1014: "MSG_CREATE_TEAM8",
    1015: "MSG_CREATE_TEAM9",
    1016: "MSG_SELECT_TEAM0",
    1017: "MSG_SELECT_TEAM1",
    1018: "MSG_SELECT_TEAM2",
    1019: "MSG_SELECT_TEAM3",
    1020: "MSG_SELECT_TEAM4",
    1021: "MSG_SELECT_TEAM5",
    1022: "MSG_SELECT_TEAM6",
    1023: "MSG_SELECT_TEAM7",
    1024: "MSG_SELECT_TEAM8",
    1025: "MSG_SELECT_TEAM9",
    1026: "MSG_ADD_TEAM0",
    1027: "MSG_ADD_TEAM1",
    1028: "MSG_ADD_TEAM2",
    1029: "MSG_ADD_TEAM3",
    1030: "MSG_ADD_TEAM4",
    1031: "MSG_ADD_TEAM5",
    1032: "MSG_ADD_TEAM6",
    1033: "MSG_ADD_TEAM7",
    1034: "MSG_ADD_TEAM8",
    1035: "MSG_ADD_TEAM9",
    1036: "MSG_DO_ATTACKSQUAD",
    1037: "MSG_DO_WEAPON",
    1038: "MSG_DO_WEAPON_AT_LOCATION",
    1039: "MSG_DO_WEAPON_AT_OBJECT",
    1040: "MSG_DO_SPECIAL_POWER",
    1041: "MSG_DO_SPECIAL_POWER_AT_LOCATION",
    1042: "MSG_DO_SPECIAL_POWER_AT_OBJECT",
    1043: "MSG_SET_RALLY_POINT",
    1044: "MSG_PURCHASE_SCIENCE",
    1045: "MSG_QUEUE_UPGRADE",
    1046: "MSG_CANCEL_UPGRADE",
    1047: "MSG_QUEUE_UNIT_CREATE",
    1048: "MSG_CANCEL_UNIT_CREATE",
    1049: "MSG_DOZER_CONSTRUCT",
    1050: "MSG_DOZER_CONSTRUCT_LINE",
    1051: "MSG_DOZER_CANCEL_CONSTRUCT",
    1052: "MSG_SELL",
    1053: "MSG_EXIT",
    1054: "MSG_EVACUATE",
    1055: "MSG_EXECUTE_RAILED_TRANSPORT",
    1056: "MSG_COMBATDROP_AT_LOCATION",
    1057: "MSG_COMBATDROP_AT_OBJECT",
    1058: "MSG_AREA_SELECTION",
    1059: "MSG_DO_ATTACK_OBJECT",
    1060: "MSG_DO_FORCE_ATTACK_OBJECT",
    1061: "MSG_DO_FORCE_ATTACK_GROUND",
    1062: "MSG_GET_REPAIRED",
    1063: "MSG_GET_HEALED",
    1064: "MSG_DO_REPAIR",
    1065: "MSG_RESUME_CONSTRUCTION",
    1066: "MSG_ENTER",
    1067: "MSG_DOCK",
    1068: "MSG_DO_MOVETO",
    1069: "MSG_DO_ATTACKMOVETO",
    1070: "MSG_DO_FORCEMOVETO",
    1071: "MSG_ADD_WAYPOINT",
    1072: "MSG_DO_GUARD_POSITION",
    1073: "MSG_DO_GUARD_OBJECT",
    1074: "MSG_DO_STOP",
    1075: "MSG_DO_SCATTER",
    1076: "MSG_INTERNET_HACK",
    1077: "MSG_DO_CHEER",
    1078: "MSG_TOGGLE_OVERCHARGE",
    1079: "MSG_SWITCH_WEAPONS",
    1080: "MSG_CONVERT_TO_CARBOMB",
    1081: "MSG_CAPTUREBUILDING",
    1082: "MSG_DISABLEVEHICLE_HACK",
    1083: "MSG_STEALCASH_HACK",
    1084: "MSG_DISABLEBUILDING_HACK",
    1085: "MSG_SNIPE_VEHICLE",
    1086: "MSG_DO_SPECIAL_POWER_OVERRIDE_DESTINATION",
    1087: "MSG_DO_SALVAGE",
    1088: "MSG_CLEAR_INGAME_POPUP_MESSAGE",
    1089: "MSG_PLACE_BEACON",
    1090: "MSG_REMOVE_BEACON",
    1091: "MSG_SET_BEACON_TEXT",
    1092: "MSG_SET_REPLAY_CAMERA",
    1093: "MSG_SELF_DESTRUCT",
    1094: "MSG_CREATE_FORMATION",
    1095: "MSG_LOGIC_CRC",
    1096: "MSG_SET_MINE_CLEARING_DETAIL",
    1097: "MSG_ENABLE_RETALIATION_MODE",
}

def read_ascii_string(file):
    bytes_read = bytearray()
    while True:
        byte = file.read(1)
        if byte == b'\x00' or not byte:
            break
        bytes_read.extend(byte)
    return bytes_read.decode('ascii')

def read_unicode_string(file):
    chars = bytearray()
    while True:
        char = file.read(2)
        if char == b'\x00\x00' or not char:
            break
        chars.extend(char)
    return chars.decode('utf-16-le')

def parse_game_message(f):
    frame_data = f.read(4)
    if not frame_data or len(frame_data) < 4:
        return None
    frame = struct.unpack('<I', frame_data)[0]

    msg_type = struct.unpack('<i', f.read(4))[0]
    player_index = struct.unpack('<i', f.read(4))[0]
    msg_text = MESSAGE_TYPE_MAP.get(msg_type, f"Unknown ({msg_type})")

    num_types_data = f.read(1)
    if not num_types_data or len(num_types_data) < 1:
        return None
    num_types = struct.unpack('<B', num_types_data)[0]

    arg_types = []
    for _ in range(num_types):
        arg_type = struct.unpack('<B', f.read(1))[0]
        arg_count = struct.unpack('<B', f.read(1))[0]
        arg_types.append((arg_type, arg_count))

    args = []
    for arg_type, arg_count in arg_types:
        fmt_size = ARG_TYPE_MAP.get(arg_type)
        if fmt_size is None:
            raise ValueError(f"Unknown argument type: {arg_type}")
        fmt, size = fmt_size
        for _ in range(arg_count):
            arg_data = f.read(size)
            if len(arg_data) < size:
                raise ValueError("Unexpected end of file while reading arguments")
            arg = struct.unpack(f'<{fmt}', arg_data)
            args.append(arg[0] if len(arg)==1 else arg)

    return {
        "frame": frame,
        "type": msg_type,
        "type_text": msg_text,
        "player_index": player_index,
        "args": args
    }

def parse_rep_file(file_path):
    with open(file_path, 'rb') as f:
        file_size = os.fstat(f.fileno()).st_size

        identifier = f.read(6).decode('ascii')
        if identifier != "GENREP":
            raise ValueError("Not a valid .rep file: missing GENREP identifier")

        start_time = struct.unpack('<I', f.read(4))[0]
        end_time = struct.unpack('<I', f.read(4))[0]
        frame_duration = struct.unpack('<I', f.read(4))[0]
        desync_game = struct.unpack('<B', f.read(1))[0]
        quit_early = struct.unpack('<B', f.read(1))[0]
        player_discons = [struct.unpack('<B', f.read(1))[0] for _ in range(MAX_SLOTS)]

        replay_name = read_unicode_string(f)
        system_time = f.read(16)
        version_string = read_unicode_string(f)
        version_time_string = read_unicode_string(f)
        version_number = struct.unpack('<I', f.read(4))[0]
        exe_crc = struct.unpack('<I', f.read(4))[0]
        ini_crc = struct.unpack('<I', f.read(4))[0]
        game_options = read_ascii_string(f)
        local_player_index_str = read_ascii_string(f)
        local_player_index = int(local_player_index_str) if local_player_index_str else -1

        difficulty = struct.unpack('<i', f.read(4))[0]
        original_game_mode = struct.unpack('<i', f.read(4))[0]
        rank_points = struct.unpack('<i', f.read(4))[0]
        max_fps = struct.unpack('<i', f.read(4))[0]

        header = {
            'start_time': start_time,
            'end_time': end_time,
            'frame_duration': frame_duration,
            'desync_game': desync_game,
            'quit_early': quit_early,
            'player_discons': player_discons,
            'replay_name': replay_name,
            'system_time': system_time.hex(),
            'version_string': version_string,
            'version_time_string': version_time_string,
            'version_number': version_number,
            'exe_crc': exe_crc,
            'ini_crc': ini_crc,
            'game_options': game_options,
            'local_player_index': local_player_index,
            'difficulty': difficulty,
            'original_game_mode': original_game_mode,
            'rank_points': rank_points,
            'max_fps': max_fps
        }

        messages = []
        msg_index = 0
        while f.tell() + 13 <= file_size:
            try:
                msg_index += 1
                message = parse_game_message(f)
                if message is None:
                    break
                messages.append(message)
            except Exception as e:
                print(f"Error parsing message {msg_index} in {file_path}: {e}")
                break

        return {'header': header, 'messages': messages}

# ----------------------------
# Parsing Player Information from Game Options
# ----------------------------
def parse_player_info(game_options):
    players = []
    pairs = game_options.split(";")
    slot = 0
    player_index_counter = 2
    for pair in pairs:
        if not pair or "=" not in pair:
            continue
        key, val = pair.split("=", 1)
        key = key.strip()
        val = val.strip()
        if key == "S":
            slots = val.split(":")
            for entry in slots:
                entry = entry.strip()
                if not entry:
                    slot += 1
                    continue
                info = {"slot": slot}  # renamed SlotOrder to slot
                # We'll remove the original "Slot" key later
                if entry[0] == "H":
                    tokens = entry.split(",")
                    name = tokens[0][1:] if tokens[0] else ""
                    if len(tokens) >= 5:
                        # For human players, use the last 5 tokens as before.
                        color = tokens[-5]
                        template = tokens[-4]
                        position = tokens[-3]
                        team = tokens[-2]
                    else:
                        color = template = position = team = ""
                    info.update({
                        "Type": "Human",
                        "Name": name,
                        "Color": color,
                        "Template": template,
                        "Position": position,
                        "Team": team,
                        "PlayerIndex": player_index_counter
                    })
                    player_index_counter += 1
                elif entry[0] == "C":
                    tokens = entry.split(",")
                    # The first token starts with 'C'. Its second character indicates difficulty.
                    ai_char = tokens[0][1] if len(tokens[0]) > 1 else ""
                    diff_map = {"E": "Easy", "M": "Medium", "H": "Brutal"}
                    ai_type = diff_map.get(ai_char, "CPU")
                    if len(tokens) >= 5:
                        # For AI, assume the following order:
                        # Token 0: AI marker (e.g., "CH")
                        # Token 1: Color (e.g., "1" meaning red)
                        # Token 2: Template (e.g., "4" meaning GLA)
                        # Token 3: Position
                        # Token 4: Team
                        color = tokens[1]
                        template = tokens[2]
                        position = tokens[3]
                        team = tokens[4]
                    else:
                        color = template = position = team = ""
                    info.update({
                        "Type": f"AI ({ai_type})",
                        "Name": "",
                        "Color": color,
                        "Template": template,
                        "Position": position,
                        "Team": team,
                        "PlayerIndex": player_index_counter
                    })
                    player_index_counter += 1
                elif entry[0] in ("O", "X"):
                    info.update({
                        "Type": "Open Slot" if entry[0] == "O" else "Closed Slot",
                        "PlayerIndex": None
                    })
                else:
                    info.update({
                        "Type": "Unknown",
                        "PlayerIndex": None
                    })
                players.append(info)
                slot += 1
    return players


def extract_seed_from_options(game_options):
    for pair in game_options.split(";"):
        pair = pair.strip()
        if pair.startswith("SD="):
            try:
                seed_str = pair[3:]
                return int(seed_str)
            except ValueError:
                return None
    return None

def assign_random_template_and_color(player_info, available_templates, available_colors, seed_value=None):
    valid_templates = [i for i in available_templates.keys() if 2 <= i <= 13]
    taken_colors = set()
    if seed_value is None:
        seed_value = GAME_LOGIC_SEED[0]

    # Pre-populate taken_colors with players that already chose a color (not "-1")
    for player in player_info:
        if player["Type"] in ("Human",) or player["Type"].startswith("AI"):
            col_val = player.get("Color", "-1")
            if col_val != "-1":
                try:
                    color_index = int(col_val)
                    if color_index in available_colors:
                        taken_colors.add(color_index)
                except ValueError:
                    pass

    for player in player_info:
        # Process only Human or AI slots.
        if player["Type"] in ("Human",) or player["Type"].startswith("AI"):
            # Template assignment:
            tpl_val = player.get("Template", "-1")
            if tpl_val == "-1":
                silly = seed_value % 7
                for _ in range(silly):
                    _ = get_game_logic_random_value(0, 1)
                idx = get_game_logic_random_value(0, 1000) % len(valid_templates)
                template_index = valid_templates[idx]
            elif tpl_val == "-2":
                # If template value is -2, assign Observer template.
                template_index = 1
            else:
                try:
                    template_index = int(tpl_val)
                    if template_index not in available_templates:
                        # If not valid, assign random.
                        silly = seed_value % 7
                        for _ in range(silly):
                            _ = get_game_logic_random_value(0, 1)
                        idx = get_game_logic_random_value(0, 1000) % len(valid_templates)
                        template_index = valid_templates[idx]
                except ValueError:
                    silly = seed_value % 7
                    for _ in range(silly):
                        _ = get_game_logic_random_value(0, 1)
                    idx = get_game_logic_random_value(0, 1000) % len(valid_templates)
                    template_index = valid_templates[idx]
            # Save as separate keys.
            player["template_index"] = template_index
            player["template"] = available_templates.get(template_index, "Unknown")
            # Remove the old Template key.
            if "Template" in player:
                del player["Template"]

            # Color assignment:
            col_val = player.get("Color", "-1")
            if col_val == "-1":
                color_index = get_game_logic_random_value(0, len(available_colors)-1)
                while color_index in taken_colors:
                    color_index = get_game_logic_random_value(0, len(available_colors)-1)
            else:
                try:
                    color_index = int(col_val)
                    if color_index not in available_colors:
                        color_index = get_game_logic_random_value(0, len(available_colors)-1)
                    # Even if already added, mark explicitly assigned color as taken.
                except ValueError:
                    color_index = get_game_logic_random_value(0, len(available_colors)-1)
            taken_colors.add(color_index)
            player["color_index"] = color_index
            player["color"] = available_colors.get(color_index, "Unknown")
            if "Color" in player:
                del player["Color"]

            # Convert position and team to integers if possible.
            pos_val = player.get("Position", "")
            team_val = player.get("Team", "")
            try:
                player["Position"] = int(pos_val)
            except (ValueError, TypeError):
                player["Position"] = None
            try:
                player["Team"] = int(team_val)
            except (ValueError, TypeError):
                player["Team"] = None

        # For non-player slots, remove unused keys.
        if "Template" in player:
            del player["Template"]
        if "Color" in player:
            del player["Color"]
        # Remove the original Slot field if it exists.
        if "Slot" in player:
            del player["Slot"]
    return player_info



# ----------------------------
# Process .rep Files and Output JSON
# ----------------------------
def process_replay_file(rep_file_path):
    parsed_data = parse_rep_file(rep_file_path)
    header = parsed_data['header']
    messages = parsed_data['messages']
    
    # Parse and assign player info
    player_info = parse_player_info(header.get('game_options', ""))
    seed_from_header = extract_seed_from_options(header.get('game_options', ""))
    if seed_from_header is not None:
        init_random(seed_from_header)
    else:
        init_random(0)
    player_info = assign_random_template_and_color(player_info, AVAILABLE_TEMPLATES, AVAILABLE_COLORS, seed_value=seed_from_header)
    
    # Determine local player info:
    local_slot = header.get('local_player_index', -1)
    local_player_index = None
    for p in player_info:
        if p.get("slot") == local_slot:
            local_player_index = p.get("PlayerIndex")
            break
    header["Local_Player_Index"] = local_player_index if local_player_index is not None else "Not found"
    
    # Combine all data into a dictionary
    data = {
        "header": header,
        "player_info": player_info,
        "messages": messages
    }
    return data

def main():
    rep_files = glob.glob("*.rep")
    if not rep_files:
        print("No .rep files found in the current directory.")
        return

    output_folder = "parsed"
    os.makedirs(output_folder, exist_ok=True)

    for rep_file in rep_files:
        try:
            print(f"Processing {rep_file}...")
            data = process_replay_file(rep_file)
            base_name = os.path.splitext(os.path.basename(rep_file))[0]
            output_file = os.path.join(output_folder, base_name + ".json")
            with open(output_file, 'w', encoding='utf-8') as out:
                json.dump(data, out, indent=4)
            print(f"Output written to {output_file}")
        except Exception as e:
            print(f"Error processing {rep_file}: {e}")

if __name__ == "__main__":
    main()
