import json
import os
import glob
import matplotlib.pyplot as plt

# Adjustable threshold for the last period as a percentage of total frames.
LAST_FRAMES_PERCENT = 0.60

# Allowed message types that are considered "spectator/dead" commands.
ALLOWED_DEAD_TYPES = {1003, 1095, 1058, 27}

def determine_game_type(data):
    """
    Determines if the game is Free For All or Team vs Team based on non-observer players.
    If all such players have Team == -1, it's Free For All; otherwise, it's Team vs Team.
    """
    players = [
        p for p in data.get("player_info", [])
        if p.get("PlayerIndex") is not None and p.get("template", "").lower() != "observer"
    ]
    if not players:
        return "Unknown"
    if all(p.get("Team", -1) == -1 for p in players):
        return "Free For All"
    else:
        return "Team vs Team"

def get_last_period_start(messages, last_frames_percent):
    """
    Computes the starting frame of the last period.
    It uses the total frame range of the messages and returns:
         last_period_start = max_frame - (total_frames * last_frames_percent)
    """
    if not messages:
        return 0
    frames = [msg.get("frame", 0) for msg in messages]
    min_frame = min(frames)
    max_frame = max(frames)
    total_frames = max_frame - min_frame
    threshold = total_frames * last_frames_percent
    return max_frame - threshold

def is_player_defeated(player_index, messages, last_period_start):
    """
    A player is considered defeated (inactive) in the last period if, for frames >= last_period_start,
    they never sent any message whose type is not in ALLOWED_DEAD_TYPES.
    """
    for msg in messages:
        if msg.get("player_index") != player_index:
            continue
        if msg.get("frame", 0) < last_period_start:
            continue
        if msg.get("type") not in ALLOWED_DEAD_TYPES:
            return False
    return True

def determine_winner(data, last_frames_percent):
    """
    Determines the winner among non-observer players.
    """
    # Filter non-observer players.
    players = [
        p for p in data.get("player_info", [])
        if p.get("PlayerIndex") is not None and p.get("template", "").lower() != "observer"
    ]
    player_by_index = {p["PlayerIndex"]: p for p in players}
    messages = data.get("messages", [])
    last_period_start = get_last_period_start(messages, last_frames_percent)
    game_type = determine_game_type(data)

    # Determine active candidates (non-defeated in the last period).
    candidates = []
    for player in players:
        if not is_player_defeated(player["PlayerIndex"], messages, last_period_start):
            candidates.append(player)
    if not candidates:
        return None, "No active (non-defeated) candidates found."

    # Helper: Check if a candidate sent MSG_SELF_DESTRUCT.
    def sent_self_destruct(player_index):
        for msg in messages:
            if msg.get("player_index") == player_index:
                if msg.get("type_text") == "MSG_SELF_DESTRUCT" or msg.get("type") == 1093:
                    return True
        return False

    # Partition candidates based on whether they never sent MSG_SELF_DESTRUCT.
    candidates_never_sd = [p for p in candidates if not sent_self_destruct(p["PlayerIndex"])]

    if candidates_never_sd:
        if len(candidates_never_sd) == 1:
            winner = candidates_never_sd[0]
            return winner, f"Winner did not send MSG_SELF_DESTRUCT. Game Type: {game_type}. Team: {winner.get('Team') if winner.get('Team', -1) != -1 else 'None'}"
        else:
            teams = {p.get("Team", -1) for p in candidates_never_sd}
            if len(teams) == 1 and list(teams)[0] != -1:
                return candidates_never_sd, f"Tie among candidates (none sent MSG_SELF_DESTRUCT) but all are on team {list(teams)[0]}. Valid team win. Game Type: {game_type}."
            else:
                return candidates_never_sd, f"Tie among candidates (none sent MSG_SELF_DESTRUCT) but they are in different teams or free-for-all. Invalid result. Game Type: {game_type}."
    else:
        # All candidates sent MSG_SELF_DESTRUCT.
        sd_frames = {}
        for msg in messages:
            if msg.get("player_index") in player_by_index:
                if msg.get("type_text") == "MSG_SELF_DESTRUCT" or msg.get("type") == 1093:
                    p_index = msg.get("player_index")
                    frame = msg.get("frame", 0)
                    sd_frames[p_index] = max(sd_frames.get(p_index, 0), frame)
        candidate_frames = [(player_by_index[p_index], frame)
                            for p_index, frame in sd_frames.items()
                            if p_index in [c["PlayerIndex"] for c in candidates]]
        if not candidate_frames:
            return None, "No self-destruct messages found among candidates."
        candidate_frames.sort(key=lambda x: x[1], reverse=True)
        highest_frame = candidate_frames[0][1]
        winners = [pf for pf in candidate_frames if pf[1] == highest_frame]
        if len(winners) == 1:
            winner = winners[0][0]
            return winner, f"Winner sent MSG_SELF_DESTRUCT at the last frame {highest_frame}. Game Type: {game_type}. Team: {winner.get('Team') if winner.get('Team', -1) != -1 else 'None'}"
        else:
            tied_players = [pf[0] for pf in winners]
            teams = {p.get("Team", -1) for p in tied_players}
            if len(teams) == 1 and list(teams)[0] != -1:
                return tied_players, f"Tie among candidates (last frame {highest_frame}) but all are on team {list(teams)[0]}. Valid team win. Game Type: {game_type}."
            else:
                return tied_players, f"Tie among candidates (last frame {highest_frame}) but they are in different teams or free-for-all. Invalid result. Game Type: {game_type}."

def process_json_file(filepath, last_frames_percent):
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None, f"Error reading file: {e}", os.path.basename(filepath), None

    # Check version
    version = data.get("header").get("version_string", "")
    if version != "Version 1.04":
        msg = f"Skipping replay {os.path.basename(filepath)} due to version mismatch: found '{version}'"
        print(msg)
        return None, msg, os.path.basename(filepath), None

    # Check for AI players.
    ai_detected = any(
        player.get("Type", "").strip().upper().startswith("AI")
        for player in data.get("player_info", [])
    )
    if ai_detected:
        msg = f"Skipping replay {os.path.basename(filepath)} because it contains an AI player."
        print(msg)
        return None, msg, os.path.basename(filepath), None

    # Collect set of templates present (non-observer players)
    match_templates = set(
        p.get("template", "Unknown") for p in data.get("player_info", [])
        if p.get("PlayerIndex") is not None and p.get("template", "").lower() != "observer"
    )

    # Proceed with winner determination.
    winner, message = determine_winner(data, last_frames_percent)
    replay_name = os.path.basename(filepath)
    return winner, message, replay_name, match_templates

def is_valid_winner(message):
    """Check if the result message indicates a valid winner."""
    invalid_indicators = ["could not be determined", "invalid result", "tie among candidates", "no active"]
    for indicator in invalid_indicators:
        if indicator.lower() in message.lower():
            return False
    return True

# Containers for results and stats.
valid_results = []  # each element: (replay_name, result_message, winner, match_templates)
total_valid_replays = 0

# Counters for exclusions.
excluded_version_count = 0
excluded_winner_count = 0

# Process all JSON files in the current directory.
directory = os.getcwd()
json_files = glob.glob(os.path.join(directory, "*.json"))
if not json_files:
    print("No JSON files found in the current directory.")
    exit(0)

for json_file in json_files:
    print(f"\nProcessing file: {json_file}")
    winner, result_message, replay_name, match_templates = process_json_file(json_file, LAST_FRAMES_PERCENT)
    print(result_message)
    # Check version mismatch or AI cases (match_templates will be None).
    if match_templates is None:
        excluded_version_count += 1
        continue

    # Only count and store replays with a valid winner.
    if winner and is_valid_winner(result_message):
        valid_results.append((replay_name, result_message, winner, match_templates))
        total_valid_replays += 1
    else:
        excluded_winner_count += 1

# Compute matchup statistics.
# For each valid match with a unique winning template, for every ordered pair (A, B) of distinct templates
# in the match, increment the match count for (A, B). If A is the winning template, also increment its win count.
matchup_stats = {}  # key: (template, opponent), value: (win_count, match_count)
for replay_name, result_message, winner, match_templates in valid_results:
    # Determine the unique winning template (skip ambiguous winners).
    if isinstance(winner, list):
        templates = {w.get("template", "Unknown") for w in winner}
        if len(templates) != 1:
            continue
        winning_template = templates.pop()
    else:
        winning_template = winner.get("template", "Unknown")
    # Ensure templates are not empty.
    match_templates = {tpl if tpl else "Unknown" for tpl in match_templates}
    for tpl in match_templates:
        for opponent in match_templates:
            if tpl == opponent:
                continue
            key = (tpl, opponent)
            win, match_count = matchup_stats.get(key, (0, 0))
            # Increment match count: this ordered pair occurred in this match.
            match_count += 1
            # If tpl is the winner in this match, count as a win.
            if tpl == winning_template:
                win += 1
            matchup_stats[key] = (win, match_count)

# Compute win rates for each matchup.
matchup_winrates = {}
for (tpl, opponent), (wins, matches) in matchup_stats.items():
    winrate = (wins / matches) * 100 if matches > 0 else 0
    matchup_winrates[(tpl, opponent)] = (winrate, matches)

# Write valid replay results and summaries to valid_winners.txt.
output_file = "valid_winners.txt"
with open(output_file, "w") as out_f:
    out_f.write(f"Total valid replays processed: {total_valid_replays}\n")
    out_f.write(f"Replays excluded due to version mismatch (or AI): {excluded_version_count}\n")
    out_f.write(f"Replays excluded due to indeterminate winner: {excluded_winner_count}\n")
    out_f.write("\n--- Valid Replay Details ---\n")
    for replay_name, result_message, winner, match_templates in valid_results:
        out_f.write(f"Replay: {replay_name}\n")
        out_f.write(f"{result_message}\n")
        if isinstance(winner, list):
            for w in winner:
                out_f.write(f"  Winner - Player Index: {w.get('PlayerIndex')}, Name: {w.get('Name')}, "
                            f"Team: {w.get('Team') if w.get('Team', -1) != -1 else 'None'}, template: {w.get('template')}\n")
        else:
            out_f.write(f"  Winner - Player Index: {winner.get('PlayerIndex')}, Name: {winner.get('Name')}, "
                        f"Team: {winner.get('Team') if winner.get('Team', -1) != -1 else 'None'}, template: {winner.get('template')}\n")
        out_f.write("  Match Templates: " + ", ".join(match_templates) + "\n")
        out_f.write("-" * 60 + "\n")
    
    out_f.write("\n--- Template vs Template Matchup Win Rates ---\n")
    for (tpl, opponent), (winrate, matches) in sorted(matchup_winrates.items()):
        out_f.write(f"  {tpl} vs {opponent}: Win Rate = {winrate:.2f}% over {matches} match(es)\n")

print(f"\nValid replay results have been written to {output_file}")

# Create a bar chart for overall win rates by winning template (aggregated from matchup stats).
wins_by_template = {}
for (tpl, opponent), (wins, matches) in matchup_stats.items():
    wins_by_template[tpl] = wins_by_template.get(tpl, 0) + wins

overall_win_rates = {}
for tpl in wins_by_template:
    # Total matches in which tpl appeared against any opponent:
    total_matches = sum(matches for (t, _), (wr, matches) in matchup_winrates.items() if t == tpl)
    overall_win_rates[tpl] = (wins_by_template[tpl] / total_matches) * 100 if total_matches > 0 else 0

templates = list(overall_win_rates.keys())
rates = [overall_win_rates[tpl] for tpl in templates]

plt.figure(figsize=(10, 6))
bars = plt.bar(templates, rates, color='skyblue')
plt.xlabel("Template")
plt.ylabel("Overall Win Rate (%)")
plt.title("Overall Win Rate by Template (Aggregated from Matchups)")
plt.ylim(0, 100)
plt.xticks(rotation=45, ha="right")
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 1, f"{yval:.1f}%", ha='center', va='bottom')
plt.tight_layout()
plt.savefig("winrate.png")
plt.close()
print("Win rate chart saved as winrate.png")
