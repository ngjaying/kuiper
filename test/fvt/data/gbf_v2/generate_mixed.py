import struct
import json

def pack_gbf(ts, type_id, items):
    # TS(8), Type(1), Count(4)
    packet = struct.pack(">QBI", ts, type_id, len(items))
    for msg_id, payload in items:
        # Item: msg_id (u16be), len (u16be), data (bytes)
        packet += struct.pack(">HH", msg_id, len(payload)) + payload
    return packet

output_file = "/home/ngjaying/repo/mande/ekbuild/test/fvt/data/gbf_v2/123.lines"

# ABI EPS (msg_id 12345)
# Fields: Tgt(20683=0x50CB), Measured(7889=0x1ED1), Reserved0(12), Lost(0), Reserved1(3), FinalReserved(0)
# Total: 24 bytes
eps_payload = struct.pack("<HH12sB3sI", 20683, 7889, b'\x00'*12, 0, b'\x00'*3, 0)

# ABI Steer (msg_id 9999)
# Fields: Angle(-1234=0xFB2E), Speed(5678=0x162E), Reserved(4)
# Total: 8 bytes
steer_payload = struct.pack("<hh4s", -1234, 5678, b'\x00'*4)

# DBC DRIVE (CAN ID 485)
# SG_ Angle : 7|8@0+ (1,0)
# SG_ Speed : 15|14@0+ (0.01,0)
# hex: Angle=100 (0x64), Speed=50.0 (5000=0x1388)
# DRIVE (485=0x1E5)
drive_payload = bytes.fromhex("6413880000000000")

# DBC GPS (CAN ID 423)
# SG_ Latitude : 7|15@0+ (0.01,-90)
# hex: Latitude=40.0 ( (40+90)/0.01 = 13000 = 0x32C8 )
# GPS (423=0x1A7)
gps_payload = bytes.fromhex("32C8000000000000")

ts = 1700000000000

packets = []
# Packet 1: ABI Mixed (EPS)
packets.append(pack_gbf(ts, 2, [(12345, eps_payload)]))

# Packet 2: DBC Multi (DRIVE + GPS)
packets.append(pack_gbf(ts+1, 1, [(485, drive_payload), (423, gps_payload)]))

# Packet 3: ABI Mixed (Steer)
packets.append(pack_gbf(ts+2, 2, [(9999, steer_payload)]))

# Packet 4: DBC Single (GPS)
packets.append(pack_gbf(ts+3, 1, [(423, gps_payload)]))

with open(output_file, "w") as f:
    for p in packets:
        f.write(p.hex() + "\n")

print(f"Generated {len(packets)} packets in {output_file}")
