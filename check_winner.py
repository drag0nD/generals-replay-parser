import os
import glob
import ujson  # faster JSON parsing
import matplotlib.pyplot as plt
import numpy as np
import concurrent.futures
import csv
import zstandard as zstd  # for decompressing .zst files



# Allowed maps set (provided list) and its lowercase version for case-insensitive checking.
ALLOWED_MAPS = {"Alpine Assault", "Barren Badlands", "Bitter Winter", "Bombardment Beach", "Desert Fury", "Dust Devil", "Final Crusade", "Flash Fire", "Forgotten Forest", "Heartland Shield", "Killing Fields", "Leipzig Lowlands", "North America", "Sand Serpent", "Seaside Mutiny", "Silent River", "The Frontline", "Tournament Desert", "Tournament Plains", "Wasteland Warlords", "Winding River", "Winter Wolf", "[RANK] Antarctic Lagoon ZH v3", "[RANK] Barren Badlands Balanced ZH v1", "[RANK] Blizzard Badlands Reloaded", "[RANK] Blizzard Badlands ZHv5", "[RANK] Bounty v3", "[RANK] Canyon of the Dead v1", "[RANK] Coastal Conflict ZH v2", "[RANK] Desert Quadrant ZH v1", "[RANK] Early Spring ZH v1", "[RANK] Evergreen Lagoon", "[RANK] Final Crusade Balanced ZH v1", "[RANK] Final Crusade FIXEDv2", "[RANK] Flash Fire Balanced ZH v1", "[RANK] Forbidden Takover ZH v2", "[RANK] Forbidden Takover ZH v3", "[RANK] Forgotten Air Battle v2", "[RANK] Forgotten Air Battle v4", "[RANK] Forgotten Ruins", "[RANK] Frog Prince ZH v2", "[RANK] Frozen Ruins", "[RANK] Highway 99 ZH v1", "[RANK] Homeland Rocks ZH v3", "[RANK] Jungle Wolf ZH v1", "[RANK] Jungle Wolf ZH v2", "[RANK] Lagoon ZH v2", "[RANK] Lagoon ZH v4", "[RANK] Liquid Gold ZH v1", "[RANK] Make Make 2 ZH v3", "[RANK] Melting Snow ZH v2", "[RANK] Melting Snow ZH v3", "[RANK] Natural Threats ZH v2", "[RANK] Sand Serpent Balanced ZH v1", "[RANK] Sand Serpent FIXED", "[RANK] Silicon Valley ZH v1", "[RANK] Snow Blind ZH v1", "TD Classic ZH v1", "TD NoBugs ZH v1", "TD OpenMiddle NoCars ZH v1", "TD OpenMiddle ZH v1", "[RANK] Abandoned Desert ZH v1", "[RANK] Ammars Sandcastles v3", "[RANK] Annihilation", "[RANK] Area J1", "[RANK] Arena of War ZH v1", "[RANK] Artic Lagoon", "[RANK] Australia ZH v1", "[RANK] Bitter Winter Balanced NoCars ZH v1", "[RANK] Bozic Destruction ZH v3", "[RANK] Cold Territory ZH v2", "[RANK] Danger Close ZH", "[RANK] DeDuSu ZH v1", "[RANK] Deserted Village v3", "[RANK] Desolated District ZH v1", "[RANK] Devastated Oasis ZH v2", "[RANK] Eagle Eye", "[RANK] Eight ZH v2", "[RANK] Embattled Land ZH v2", "[RANK] Forest of Oblivion ZH v1", "[RANK] Gold Cobra", "[RANK] Hard Winter ZH v2", "[RANK] Hidden Treasures v2", "[RANK] Homeland Rocks ZH v4", "[RANK] Irish Front ZH v1", "[RANK] Liquid Gold ZH v2", "[RANK] Lost Valley v2", "[RANK] Mountain Mayhem v2", "[RANK] Mountain Oil ZH v1", "[RANK] Natural Threats ZH v3", "[RANK] Onza Map v1", "[RANK] Rebellion ZH v1", "[RANK] Sand Scorpion", "[RANK] Scaraa ZH v1", "[RANK] Scorched Earth ZH v3", "[RANK] Sleeping Dragon v3", "[RANK] Snow Aggression v3", "[RANK] Snow Blind ZH v2", "[RANK] Snowy Drought v4", "[RANK] Storm Valley", "[RANK] TD Classic NoCars ZH v1", "[RANK] TD NoBugsCars ZH v1", "[RANK] Total Domination No SDZ ZH v1", "[RANK] Tournament Delta ZH v2", "[RANK] Uneven Heights v3", "[RANK] Vendetta ZH v1", "[RANK] Wasteland Warlords Revised", "[RANK] Winter Arena", "[RANK] Winter Wolf Balanced ZH v1", "[RANK] ZH Carrier is Over v2", "[RANK] [NMC] Battle on the River", "[RANK] [NMC] Blasted Lands", "[RANK] [NMC] Summer Arena", "[RANK] [NMC] Tournament Arena", "[RANK] [NMC] Tournament City", "[RANK] A New Tragedy ZH v1", "[RANK] Abandoned Farms ZH v1", "[RANK] Barren Badlands Balanced ZH v2", "[RANK] Battle Plan ZH v1", "[RANK] Blossoming Valley ZH v1", "[RANK] Canyon of the Dead ZH v2", "[RANK] Combat Island ZH v1", "[RANK] Down the Road ZH v1", "[RANK] Drallim Desert ZH v2", "[RANK] Early Spring ZH v2", "[RANK] Egyptian Oasis ZH v1", "[RANK] Farmlands of the Fallen ZH v1", "[RANK] Final Crossroad ZH v1", "[RANK] Forgotten Air Battle ZH v5", "[RANK] A New Tragedy ZH v2", "[RANK] AKAs Magic ZH v1", "[RANK] Alfies Haven ZH v1", "[RANK] Arctic Arena ZH v1", "[RANK] Arctic Lagoon ZH v2", "[RANK] Arizona Airfield ZH v1", "[RANK] Battleship Bay ZH v1", "[RANK] Black Hell ZH v1", "[RANK] Blue Hole ZH v1", "[RANK] Dammed Cottages ZH v1", "[RANK] Dammed Korhal ZH v1", "[RANK] Dammed Scorpion ZH v1", "[RANK] Desert Fury ZH v1", "[RANK] Double Damination ZH v1", "[RANK] Dry River ZH v1", "[RANK] Dust Devil ZH v1", "[RANK] Echo ZH v1", "[RANK] Endboss ZH v1", "[RANK] Fort Payne ZH v1", "[RANK] Hanamura Temple ZH v1", "[RANK] Natural Threats ZH v4", "[RANK] Proxy War ZH v1", "[RANK] Rainforest Reservoir ZH v1", "[RANK] Sacred Land ZH v1", "[RANK] Sakura Forest II ZH v1", "[RANK] Salt Lake River ZH v1", "[RANK] Snowy Drought ZH v5", "[RANK] Snowy Roads ZH v1", "[RANK] Storm Surge ZH v1", "[RANK] Tiny Tactics ZH v1", "[RANK] Winding River Revised ZH v1", "[RANK] Yelling Avalanche ZH v1", "1v1 try it_v2a", "1v1 try it_v2b", "Additional Forces ZH v1", "Alpine Assault v2", "Battle Park", "Bitter Winter Balanced ZH vB", "ButterbroT ZH v3", "Canyon Frost ZH v1", "Crazy Beach ZH v1", "Cross-Country", "Down the Road v3", "Drallim Desert v1", "Entropys Empire ZH v2", "Forest of Camelot ZH v1", "Freezing Rain v1", "Frozen Dawn ZH v1 (draf02)", "GenTools secret Lab A", "Jammed Lands {v3}", "Killing Fields Balanced v2", "Kinky Fields ZH v1", "Leipzig Lowlands Balanced ZH vB", "Lone Outpost", "Make-Make ZHv2", "Modern Warfare ZH v0", "Mountain Arena ZH v1", "Poseidons Lair ZH v2", "Rising Legion ZH v1", "Salt Lake River ZH v1", "Siege(Tower)", "Sleeping Dragon V2", "Snow Land Nation ZH v3", "Snowy Plateau v2", "Stonehenge ZH TEST v4", "Tournament in Canyon V2", "Tournament in Canyon V32", "Urban Pinch ZH v1", "Wings of Fury", "Wrong Neighborhood v1", "Yota Nation Arena ZH v1", "Yota Nation Battleground ZH v1", "[RANK] Bozic Destruction ZH v4", "[RANK] Tournament Delta ZH v3"}
ALLOWED_MAPS_LOWER = {m.lower() for m in ALLOWED_MAPS}

# Global counters for skipped replays (each replay is counted only once based on priority).
skipped_maps_count = 0
skipped_low_duration_count = 0
skipped_ai_count = 0
skipped_attack_object_count = 0
skipped_winner_count = 0

# Also, record unique map names that were skipped.
skipped_map_names = set()

def determine_game_type(data):
    players = [p for p in data.get("player_info", [])
               if p.get("PlayerIndex") is not None and p.get("template", "").lower() != "observer"]
    if not players:
        return "Unknown"
    if all(p.get("Team", -1) == -1 for p in players):
        return "Free For All"
    return "Team vs Team"

def determine_winner(data):
    """
    Determines the winner among non-observer players.
    First, candidates that never sent a self-destruct message are preferred.
    If all candidates sent a self-destruct, then the one(s) with the latest self-destruct frame is/are chosen.
    Returns a winner (a dict or a list of dicts) and a message detailing which criteria were used.
    """
    players = [p for p in data.get("player_info", [])
               if p.get("PlayerIndex") is not None and p.get("template", "").lower() != "observer"]
    messages = data.get("messages", [])
    game_type = determine_game_type(data)

    def sent_self_destruct(player_index):
        for msg in messages:
            if msg.get("player_index") == player_index:
                if msg.get("type_text") == "MSG_SELF_DESTRUCT" or msg.get("type") == 1093:
                    return True
        return False

    # Prefer candidates that never sent self-destruct.
    candidates_never_sd = [p for p in players if not sent_self_destruct(p["PlayerIndex"])]
    if candidates_never_sd:
        if len(candidates_never_sd) == 1:
            return candidates_never_sd[0], f"Winner determined because only one candidate never sent self-destruct. Game Type: {game_type}."
        else:
            teams = {p.get("Team", -1) for p in candidates_never_sd}
            if len(teams) == 1 and list(teams)[0] != -1:
                return candidates_never_sd, f"Tie among candidates (none sent self-destruct) but all on team {list(teams)[0]}. Valid team win. Game Type: {game_type}."
            else:
                return candidates_never_sd, f"Tie among candidates (none sent self-destruct) but different teams or free-for-all. Invalid result. Game Type: {game_type}."
    else:
        # Use self-destruct frame times.
        sd_frames = {}
        for msg in messages:
            if msg.get("player_index") is None:
                continue
            if msg.get("type_text") == "MSG_SELF_DESTRUCT" or msg.get("type") == 1093:
                p_index = msg.get("player_index")
                frame = msg.get("frame", 0)
                sd_frames[p_index] = max(sd_frames.get(p_index, 0), frame)
        candidate_frames = [(p, sd_frames.get(p["PlayerIndex"], 0)) for p in players if p["PlayerIndex"] in sd_frames]
        if not candidate_frames:
            return None, "No self-destruct messages found among candidates."
        candidate_frames.sort(key=lambda x: x[1], reverse=True)
        highest_frame = candidate_frames[0][1]
        winners = [pf for pf in candidate_frames if pf[1] == highest_frame]
        if len(winners) == 1:
            return winners[0][0], f"Winner determined because one candidate had the latest self-destruct at frame {highest_frame}. Game Type: {game_type}."
        else:
            tied_players = [pf[0] for pf in winners]
            teams = {p.get("Team", -1) for p in tied_players}
            if len(teams) == 1 and list(teams)[0] != -1:
                return tied_players, f"Tie among candidates at frame {highest_frame} but all on team {list(teams)[0]}. Valid team win. Game Type: {game_type}."
            else:
                return tied_players, f"Tie among candidates at frame {highest_frame} but different teams or free-for-all. Invalid result. Game Type: {game_type}."

def get_unique_winner_template(winner):
    """
    Given the winner (a dict or a list of dicts), extract the winning template.
    Returns the template if unique; otherwise, returns None.
    """
    if winner is None:
        return None
    if isinstance(winner, list):
        templates = {w.get("template", "Unknown") for w in winner}
        if len(templates) == 1:
            return templates.pop()
        return None
    return winner.get("template", "Unknown")

def process_json_file(filepath):
    """
    Reads a replay file (a .zst file) and performs AI, frame_duration, desync_game,
    MSG_DO_ATTACK_OBJECT filtering, and map filtering.
    Determines the winner.
    Computes:
      - Game duration in minutes (using header['frame_duration'], 1 sec = 30 frames)
      - Actions per minute (total actions / duration in minutes)
    Returns a tuple:
      (winner, message, replay_name, match_templates, duration_minutes, actions_per_minute, skip_reason)
      If the replay is valid, skip_reason is None.
    """
    try:
        with open(filepath, 'rb') as f:
            dctx = zstd.ZstdDecompressor()
            data_bytes = dctx.decompress(f.read())
            data = ujson.loads(data_bytes)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None, f"Error reading file: {e}", os.path.basename(filepath), None, None, None, "read_error"

    replay_name = os.path.basename(filepath)
    header = data.get("header", {})
    frame_duration = header.get("frame_duration", None)
    duration_minutes = frame_duration / 1800 if frame_duration is not None else None

    # Priority 1: Map filtering.
    map_name = header.get("map", "Unknown")
    if map_name.lower() not in ALLOWED_MAPS_LOWER:
        msg = f"Skipping replay {replay_name} because map '{map_name}' is not allowed."
        print(msg)
        global skipped_maps_count, skipped_map_names
        skipped_maps_count += 1
        skipped_map_names.add(map_name)
        return None, msg, replay_name, None, duration_minutes, None, "disallowed_map"

    # Priority 2: Low duration.
    if frame_duration is None or frame_duration < 3000:
        msg = f"Skipping replay {replay_name} due to low frame_duration {frame_duration} (<3000)."
        print(msg)
        return None, msg, replay_name, None, duration_minutes, None, "low_duration"

    # Priority 3: Desync check.
    if header.get("desync_game", 0) == 1:
        msg = f"Skipping replay {replay_name} because desync_game == 1."
        print(msg)
        return None, msg, replay_name, None, duration_minutes, None, "desync_game"

    # Priority 4: For each non-observer player, ensure they sent MSG_DO_ATTACK_OBJECT (type 1059).
    players = [p for p in data.get("player_info", [])
               if p.get("PlayerIndex") is not None and p.get("template", "").lower() != "observer"]
    for player in players:
        pid = player["PlayerIndex"]
        if not any(m.get("type") == 1059 and m.get("player_index") == pid for m in data.get("messages", [])):
            msg = (f"Skipping replay {replay_name} because player {player.get('Name', 'Unknown')} "
                   f"(index {pid}) did not send MSG_DO_ATTACK_OBJECT.")
            print(msg)
            global skipped_attack_object_count
            skipped_attack_object_count += 1
            return None, msg, replay_name, None, duration_minutes, None, "no_attack_object"

    # Priority 5: AI filtering.
    ai_detected = any(
        player.get("Type", "").strip().upper().startswith("AI")
        for player in data.get("player_info", [])
    )
    if ai_detected:
        msg = f"Skipping replay {replay_name} because it contains an AI player."
        print(msg)
        return None, msg, replay_name, None, duration_minutes, None, "ai_player"

    match_templates = set(
        p.get("template", "Unknown") for p in data.get("player_info", [])
        if p.get("PlayerIndex") is not None and p.get("template", "").lower() != "observer"
    )

    winner, win_msg = determine_winner(data)
    template = get_unique_winner_template(winner)
    if template is None:
        msg = f"Replay {replay_name} skipped due to indeterminate winner. Reason: {win_msg}"
        print(msg)
        return None, msg, replay_name, match_templates, duration_minutes, None, "indeterminate_winner"

    combined_message = f"SUCCESS: {replay_name} processed. Winner faction: {template}. {win_msg}"
    print(combined_message)

    # Count player actions: count messages with a valid player_index.
    total_actions = sum(1 for m in data.get("messages", []) if m.get("player_index") is not None)
    # Calculate actions per minute: total_actions / duration_minutes
    actions_per_minute = total_actions / duration_minutes if duration_minutes and duration_minutes > 0 else None

    return winner, combined_message, replay_name, match_templates, duration_minutes, actions_per_minute, None

def is_valid_winner(message):
    invalid_indicators = ["could not be determined", "invalid result", "tie among candidates", "no active"]
    return not any(indicator.lower() in message.lower() for indicator in invalid_indicators)

# --- Process files concurrently using ThreadPoolExecutor ---
zst_files = glob.glob(os.path.join(os.getcwd(), "*.zst"))
total_files = len(zst_files)
print(f"Found {total_files} .zst files.\n")

results = []
valid_results = []  # (replay_name, message, winner, match_templates, duration_minutes, actions_per_minute)
total_valid_replays = 0

# We'll update our skip counters only once per replay based on the returned skip_reason.
for_reason = {
    "disallowed_map": 0,
    "low_duration": 0,
    "desync_game": 0,
    "no_attack_object": 0,
    "ai_player": 0,
    "indeterminate_winner": 0,
    "read_error": 0
}

def process_file_wrapper(filepath):
    return process_json_file(filepath)

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = {executor.submit(process_file_wrapper, f): f for f in zst_files}
    for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
        try:
            result = future.result()
            results.append(result)
            file_name = os.path.basename(futures[future])
            print(f"Processed file {idx}/{total_files}: {file_name}")
        except Exception as exc:
            print(f"File {futures[future]} generated an exception: {exc}")

for winner, result_message, replay_name, match_templates, duration_minutes, actions_per_minute, skip_reason in results:
    if skip_reason is not None:
        for_reason[skip_reason] += 1
        continue
    if match_templates is None or winner is None or not is_valid_winner(result_message):
        for_reason["indeterminate_winner"] += 1
        continue
    valid_results.append((replay_name, result_message, winner, match_templates, duration_minutes, actions_per_minute))
    total_valid_replays += 1

print(f"\nProcessing complete: {total_files} files processed.")
print(f"Total valid replays: {total_valid_replays}")
print(f"Skipped due to disallowed map: {for_reason['disallowed_map']}")
print(f"Skipped due to low duration: {for_reason['low_duration']}")
print(f"Skipped due to desync game: {for_reason['desync_game']}")
print(f"Skipped due to AI: {for_reason['ai_player']}")
print(f"Skipped due to no MSG_DO_ATTACK_OBJECT: {for_reason['no_attack_object']}")
print(f"Skipped due to indeterminate winner: {for_reason['indeterminate_winner']}")
print(f"Skipped due to read errors: {for_reason['read_error']}")

# Write unique skipped map names to a separate text file.
skipped_maps_file = "skipped_maps.txt"
with open(skipped_maps_file, "w", encoding="utf-8") as map_out:
    for map_name in sorted(skipped_map_names):
        map_out.write(map_name + "\n")
print(f"Unique skipped map names have been written to {skipped_maps_file}")

# Compute average actions per minute for all valid replays.
all_actions_per_minute = [apm for _, _, _, _, _, apm in valid_results if apm is not None]
avg_actions_per_minute = np.mean(all_actions_per_minute) if all_actions_per_minute else None

# Compute matchup statistics.
matchup_stats = {}
for replay_name, result_message, winner, match_templates, duration_minutes, actions_per_minute in valid_results:
    if isinstance(winner, list):
        templates_set = {w.get("template", "Unknown") for w in winner}
        if len(templates_set) != 1:
            continue
        winning_faction = templates_set.pop()
    else:
        winning_faction = winner.get("template", "Unknown")
    match_templates = {tpl if tpl else "Unknown" for tpl in match_templates}
    for tpl in match_templates:
        for opponent in match_templates:
            if tpl == opponent:
                continue
            key = (tpl, opponent)
            win, match_count = matchup_stats.get(key, (0, 0))
            match_count += 1
            if tpl == winning_faction:
                win += 1
            matchup_stats[key] = (win, match_count)

# Deduplicate mirror matchups.
deduped_matchups = {}
processed_pairs = set()
for (faction, opponent), (wins, matches) in matchup_stats.items():
    pair = frozenset([faction, opponent])
    if pair in processed_pairs:
        continue
    if (opponent, faction) in matchup_stats:
        wins_rev, matches_rev = matchup_stats[(opponent, faction)]
        rate = (wins / matches) * 100 if matches > 0 else 0
        rate_rev = (wins_rev / matches_rev) * 100 if matches_rev > 0 else 0
        if rate >= rate_rev:
            deduped_matchups[(faction, opponent)] = (rate, matches)
        else:
            deduped_matchups[(opponent, faction)] = (rate_rev, matches_rev)
    else:
        rate = (wins / matches) * 100 if matches > 0 else 0
        deduped_matchups[(faction, opponent)] = (rate, matches)
    processed_pairs.add(pair)

# Write summary to valid_winners.txt.
output_file = "valid_winners.txt"
with open(output_file, "w", encoding="utf-8") as out_f:
    if avg_actions_per_minute is not None:
        out_f.write(f"Average Actions per Minute: {avg_actions_per_minute:.2f}\n")
    out_f.write(f"Total valid replays processed: {total_valid_replays}\n")
    out_f.write(f"Replays skipped due to disallowed map: {for_reason['disallowed_map']}\n")
    out_f.write(f"Replays skipped due to low duration: {for_reason['low_duration']}\n")
    out_f.write(f"Replays skipped due to desync game: {for_reason['desync_game']}\n")
    out_f.write(f"Replays skipped due to AI: {for_reason['ai_player']}\n")
    out_f.write(f"Replays skipped due to no MSG_DO_ATTACK_OBJECT: {for_reason['no_attack_object']}\n")
    out_f.write(f"Replays skipped due to indeterminate winner: {for_reason['indeterminate_winner']}\n")
    out_f.write(f"Replays skipped due to read errors: {for_reason['read_error']}\n")
    out_f.write("\n--- Valid Replay Details ---\n")
    for replay_name, result_message, winner, match_templates, duration_minutes, actions_per_minute in valid_results:
        out_f.write(f"Replay: {replay_name}\n")
        out_f.write(f"{result_message}\n")
        if isinstance(winner, list):
            for w in winner:
                out_f.write(f"  Winner - Player Index: {w.get('PlayerIndex')}, Name: {w.get('Name')}, faction: {w.get('template')}\n")
        else:
            out_f.write(f"  Winner - Player Index: {winner.get('PlayerIndex')}, Name: {winner.get('Name')}, faction: {winner.get('template')}\n")
        out_f.write("  Match Factions: " + ", ".join(match_templates) + "\n")
        if duration_minutes is not None:
            out_f.write(f"  Match Duration: {duration_minutes:.2f} minute(s)\n")
        if actions_per_minute is not None:
            out_f.write(f"  Actions per Minute: {actions_per_minute:.2f}\n")
        out_f.write("-" * 60 + "\n")
    
    out_f.write("\n--- Faction vs Faction Matchup Win Rates ---\n")
    for (faction, opponent), (winrate, matches) in sorted(deduped_matchups.items()):
        out_f.write(f"  {faction} vs {opponent}: Win Rate = {winrate:.2f}% over {matches} match(es)\n")

print(f"\nValid replay results have been written to {output_file}")

# -------------------------------
# Generate the Overall Win Rate Chart by Faction.
wins_by_faction = {}
for (faction, opponent), (wins, matches) in matchup_stats.items():
    wins_by_faction[faction] = wins_by_faction.get(faction, 0) + wins

overall_win_rates = {}
for faction in wins_by_faction:
    total_matches = sum(matches for (f, _), (wr, matches) in matchup_stats.items() if f == faction)
    overall_win_rates[faction] = (wins_by_faction[faction] / total_matches) * 100 if total_matches > 0 else 0

factions_sorted = sorted(overall_win_rates.keys())
rates = [overall_win_rates[f] for f in factions_sorted]

plt.figure(figsize=(10, 6))
bars = plt.bar(factions_sorted, rates, color='skyblue')
plt.xlabel("Faction")
plt.ylabel("Overall Win Rate (%)")
plt.title("Overall Win Rate by Faction (Aggregated from Matchups)")
plt.ylim(0, 100)
plt.xticks(rotation=45, ha="right")
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 1, f"{yval:.1f}%", ha='center', va='bottom')
plt.tight_layout()
plt.savefig("overall_winrate.png")
plt.close()
print("Overall win rate chart saved as overall_winrate.png")

# -------------------------------
# Generate a 100% Stacked Bar Chart for Faction vs Faction Matchups.
matchup_labels = []
win_rates = []
loss_rates = []
for (faction, opponent), (winrate, matches) in sorted(deduped_matchups.items()):
    matchup_labels.append(f"{faction} vs {opponent}")
    win_rates.append(winrate)
    loss_rates.append(100 - winrate)

y = np.arange(len(matchup_labels))
bar_height = 0.5

plt.figure(figsize=(10, max(6, len(matchup_labels) * 0.3)))
bars_win = plt.barh(y, win_rates, height=bar_height, color='green', label='Win')
bars_loss = plt.barh(y, loss_rates, height=bar_height, left=win_rates, color='red', label='Loss')
plt.yticks(y, matchup_labels)
plt.xlabel("Win Rate (%)")
plt.title("100% Stacked Bar Chart for Faction vs Faction Matchups")
plt.legend()
plt.xlim(0, 100)
for i, (w_rate, bar) in enumerate(zip(win_rates, bars_win)):
    x_center = w_rate / 2
    if w_rate < 15:
        plt.text(w_rate + 1, bar.get_y() + bar.get_height()/2, f"{w_rate:.1f}%", va='center', color='black', fontsize=8)
    else:
        plt.text(x_center, bar.get_y() + bar.get_height()/2, f"{w_rate:.1f}%", va='center', ha='center', color='white', fontsize=8)
plt.tight_layout()
plt.savefig("matchup_stacked.png")
plt.close()
print("Stacked matchup chart saved as matchup_stacked.png")

# -------------------------------
# Generate Relationship Table Image.
faction_list = sorted(list(overall_win_rates.keys()))
n = len(faction_list)
table_data = []
for i in range(n):
    row = []
    for j in range(n):
        if i == j:
            row.append("100%")
        else:
            key = (faction_list[i], faction_list[j])
            if key in matchup_stats and matchup_stats[key][1] > 0:
                win, match_count = matchup_stats[key]
                rate = (win / match_count) * 100
                row.append(f"{rate:.1f}%")
            else:
                row.append("N/A")
    table_data.append(row)

fig, ax = plt.subplots(figsize=(1 + n, 1 + n))
ax.axis('tight')
ax.axis('off')
table = ax.table(cellText=table_data, rowLabels=faction_list, colLabels=faction_list, loc='center')
table.auto_set_font_size(False)
table.set_fontsize(8)
table.scale(1.2, 1.2)
plt.title("Relationship Table: Faction vs Faction Win Rates", fontsize=12)
plt.savefig("relationship_table.png", bbox_inches='tight')
plt.close()
print("Relationship table image saved as relationship_table.png")

# -------------------------------
# NEW FUNCTIONALITY: Average Time to Win/Loss per Matchup
win_times = {}   # key: (faction, opponent) -> list of win durations (in minutes)
loss_times = {}  # key: (faction, opponent) -> list of loss durations (in minutes)

for replay_name, result_message, winner, match_templates, duration_minutes, actions_per_minute in valid_results:
    if duration_minutes is None:
        continue
    winning_faction = get_unique_winner_template(winner)
    if len(match_templates) == 1:
        tpl = next(iter(match_templates))
        key = (tpl, tpl)
        win_times.setdefault(key, []).append(duration_minutes)
        loss_times.setdefault(key, []).append(duration_minutes)
    else:
        for tpl in match_templates:
            for opponent in match_templates:
                key = (tpl, opponent)
                if tpl == winning_faction:
                    win_times.setdefault(key, []).append(duration_minutes)
                else:
                    loss_times.setdefault(key, []).append(duration_minutes)

all_factions = set()
for _, _, _, match_templates, _, _ in valid_results:
    all_factions.update(match_templates)
all_factions = sorted(list(all_factions))
num_factions = len(all_factions)

avg_win_matrix = np.full((num_factions, num_factions), np.nan)
avg_loss_matrix = np.full((num_factions, num_factions), np.nan)
annotation_matrix = [["" for _ in range(num_factions)] for _ in range(num_factions)]

for i, f in enumerate(all_factions):
    for j, opp in enumerate(all_factions):
        key = (f, opp)
        avg_win = np.mean(win_times.get(key, [np.nan]))
        avg_loss = np.mean(loss_times.get(key, [np.nan]))
        avg_win_matrix[i, j] = avg_win
        avg_loss_matrix[i, j] = avg_loss
        if not np.isnan(avg_win) and not np.isnan(avg_loss):
            annotation_matrix[i][j] = f"{avg_win:.1f}/{avg_loss:.1f}"
        else:
            annotation_matrix[i][j] = "N/A"

# Create Heatmap for Average Time to Win (minutes)
plt.figure(figsize=(12, 10))
im = plt.imshow(avg_win_matrix, cmap="viridis", interpolation="nearest")
plt.colorbar(im, label="Average Win Time (minutes)")
plt.xticks(np.arange(num_factions), all_factions, fontsize=10)
plt.yticks(np.arange(num_factions), all_factions, fontsize=10)
plt.title("Heatmap: Average Time to Win (minutes)\n(Annotations: win time / loss time)", fontsize=12)
for i in range(num_factions):
    for j in range(num_factions):
        plt.text(j, i, annotation_matrix[i][j],
                 ha="center", va="center", color="w", fontsize=10, rotation=45)
plt.tight_layout()
plt.savefig("matchup_time_heatmap.png")
plt.close()
print("Heatmap for average time to win/loss saved as matchup_time_heatmap.png")

# Create Table Image for Average Time to Win/Loss per Matchup.
fig, ax = plt.subplots(figsize=(12, 10))
ax.axis('tight')
ax.axis('off')
table = ax.table(cellText=annotation_matrix,
                 rowLabels=all_factions,
                 colLabels=all_factions,
                 loc='center')
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(2, 2)
plt.title("Table: Average Time (minutes) to Win/Loss (win/loss)", fontsize=12)
for key, cell in table.get_celld().items():
    cell.get_text().set_fontsize(10)
plt.savefig("matchup_time_table.png", bbox_inches='tight')
plt.close()
print("Table for average time to win/loss saved as matchup_time_table.png")
