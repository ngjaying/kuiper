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
---

## 动态长度数据（DDS Sequence）封包方案

对于包含动态长度缓冲区（如 `dds_sequence_uint8` 或 `String`）的结构体，由于 C 标准 ABI 的指针在跨进程网络传输时没有实际意义，为了保持封包高效并实现“零拷贝”级别的开销，我们采用**原生内存结构体追加与指针偏移替换（Pointer Swizzling）**方案。

### 原理：代理端直接覆盖指针
在 64 位系统下，`dds_sequence_uint8` 结构体的自然内存对齐大小刚好是 24 字节：
* `_maximum` (uint32_t, 4 字节)
* `_length` (uint32_t, 4 字节)
* `_buffer` *指针* (8 字节) —— **代理端将修改这 8 个字节**
* `_release` (1 字节 + 7 字节内存对齐，共 8 字节)

### 代理端（Sender）操作步骤

为了避免由于动态字段导致的逐个字段手动序列化（极大影响性能），代理端（Proxy）只需执行以下高效操作：

1. **直接内存拷贝（Memcopy）**：将整个包含 sequence 的 C 结构体原封不动地 `memcpy` 到网络封包缓冲区 payload 中（这保留了原本所有的 C 内存布局对齐规则）。
2. **追加真实数据（Append）**：从内存中找到该 `_buffer` 原本指向的真实数据数据块，将其长度为 `_length` 的字节数组直接拷贝，并追加在当前生成 payload 的**最末尾**。
3. **替换指针为偏移量（Swizzle）**：回到刚刚 payload 中的 sequence 头部，将其中的 8 字节 `_buffer` 指针原内存地址，直接覆盖改写为一个 **`uint64` 整数（偏移量 offset）**。该 offset 表示刚刚追加在尾部的数据块在当前 payload 中的起始字节位置。

#### 封包最终 Payload 形态：
```
[固定字段 1...] 
[固定字段 2...]
[ 24 字节 dds_sequence_uint8 的结构体 {
     _maximum: [4 字节]
     _length:  [4 字节]
     _buffer:  [8 字节] <---- (⚠️ 被覆盖为一个 uint64 偏移量)
     _release: [8 字节]
} ]
[后续其它的所有固定字段...]
[刚刚追加的 _buffer 指向的真实变长数据块...]  <---- offset 指向这块区域
```

### 为什么选择该方案？
* **代理端无需重新装箱**：代理端不需要写 any 繁琐的序列化逻辑或破坏现有对齐，只做 1 个 `memcpy`，1 个数据 `Append` 和 1 个 `Offset 覆写` 即可完成动态长度传递，性能最高。
* **eKuiper 解码零代价**：在 eKuiper 端 `decode.go` 只需增加一个特定基础类型。因为结构体本身的大小（24 字节）被完美保留，所以后续所有其他字段的静态计算偏移量 offset 不会受到任何影响！解码器读取 8 字节的偏移量和前置的 `_length` 后，直接从 payload 尾部切片获取字符串，安全且属于 $O(1)$ 级极速解析。
