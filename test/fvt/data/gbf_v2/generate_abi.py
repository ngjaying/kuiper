import json
import struct

summary_file = '/home/ngjaying/repo/mande/ekuiper_can/requirement/123_summary.json'
output_dir = '/home/ngjaying/repo/mande/ekbuild/test/fvt/data/gbf_v2'

with open(summary_file, 'r') as f:
    data = json.load(f)

structs = {}
mapping_entries = []

target_key = "Basc_EPSBasInfo_Proxy.Ntf_EPSBasInfo"
target_info = data["services"].get(target_key)

if target_info:
    size = target_info["declared_size"]
    structs[target_key] = {
        "fields": [
            {"name": "raw_data", "type": f"uint8[{size}]"}
        ]
    }
    
    mapping_entries.append({
        "msg_id": 12345, # mock msg_id
        "type": 2,
        "key": target_key
    })

    with open(f"{output_dir}/123.lines", "w") as out_txt:
        for sample in target_info["samples"][:5]:
            payload = bytes.fromhex(sample)
            
            ts = int(1600000000)
            type_id = 2
            item_count = 1 
            
            packet = struct.pack(">QBI", ts, type_id, item_count)
            
            msg_id = 12345
            payload_len = len(payload)
            frame = struct.pack(">HH", msg_id, payload_len) + payload
            
            packet += frame
            
            out_txt.write(packet.hex() + "\n")

    abi_schema_json = {
        "endian": "little",
        "structs": structs
    }

    with open(f"{output_dir}/unified_v1.abi.json", "w") as f:
        json.dump(abi_schema_json, f, indent=4)

    mapping_json = {
        "version": "v1",
        "entries": mapping_entries
    }

    with open(f"{output_dir}/msg_id_map.json", "w") as f:
        json.dump(mapping_json, f, indent=4)

print("Generation complete.")
