# 数据代理封包指南

## 快速步骤

### CAN 数据（type_id = 0）

CAN-DDS 原始数据不需要任何改动，代理只需在前面追加 **13 字节包头**：

```
步骤 1: 构造包头
  [packet_ts  8字节, 大端序 uint64]  ← 当前时间戳
  [type_id    1字节, 值=0x00]        ← CAN 类型
  [total_len  4字节, 大端序 uint32]  ← 原始 CAN-DDS 载荷的总字节数

步骤 2: 拼接
  发送 = 包头(13B) + 原始CAN-DDS载荷（不做任何修改）
```

---

### Service 数据（type_id = 2）

每收到一个 DDS Service 通知，按以下步骤封装帧，可将多帧打包到同一个包中：

```
步骤 1: 查表获取 msg_id
  用 "Service名.Method名" 在 msg_id_mapping.json 中查找 msg_id

步骤 2: 构造帧（每个通知一帧，可多帧）
  [msg_id  2字节, 大端序 uint16]  ← 从映射表查到的 msg_id
  [len     2字节, 大端序 uint16]  ← 载荷字节数
  [payload len字节]               ← C 结构体原始二进制

步骤 3: 构造包头
  [packet_ts  8字节]   ← 当前时间戳
  [type_id    1字节]   ← 值=0x02
  [total_len  4字节]   ← 所有帧的字节总和

步骤 4: 拼接
  发送 = 包头(13B) + 帧1 + 帧2 + ...
```

---

### Proto 数据（type_id = 1）

与 Service 完全一致，仅 `type_id = 0x01`，payload 为 protobuf 编码。

---

## 注意事项

1. **字节序**：所有多字节字段使用大端序（Big Endian）
2. **CAN 零拷贝**：CAN 原始载荷不做任何修改
3. **类型不混包**：每个包只包含一种 type_id
4. **映射文件同步**：代理和 eKuiper 必须使用同一份 `msg_id_mapping.json`

## 映射文件生成

```bash
go run tools/msgid_gen/main.go \
  -csv requirement/all.csv \
  -csv requirement/all2.csv \
  -o msg_id_mapping.json
```
