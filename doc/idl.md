# 如何使用 IDL (Interface Description Language) Schema

eKuiper 平台通过支持 IDL (Interface Description Language) schema，提供了一种定义复杂数据结构的强大机制。此功能尤其适用于高效处理和解析二进制数据流，例如来自 CAN 总线或 SPI 接口的原始数据。IDL schema 的应用确保了原始字节流能够被精确地转换为结构化、可读的数据格式。

## 操作指南

本指南旨在详细阐述以下核心操作流程：

1.  IDL schema 的创建。

2.  基于 IDL schema 的流定义。

3.  规则的构建与数据处理。

### 前提条件

* eKuiper 实例须处于运行状态。

* MQTT Broker 服务应可访问。

### 1. 创建 IDL Schema

IDL schema 通常以 `.idl` 或其他特定于 IDL 的文件扩展名形式存在。eKuiper 提供了两种导入 schema 的机制：通过统一资源定位符 (URL) 引用，或直接提供其文本内容。在本示例中，我们将演示如何通过引用包含 IDL schema 及其相关支持文件的 ZIP 归档来创建 schema。

**创建方法：**

IDL schema 的创建通过向 eKuiper 的 `/schemas/idl` 端点发送 `POST` 请求实现。请求体中必须包含 schema 的名称 (`name`)、类型（固定为 `idl`）以及指向 schema 文件或 ZIP 归档的路径 (`file`)。此文件路径可采用 `http`、`https` 或 `file` 协议方案。

**REST API 请求示例：**

```

POST http://localhost:9020/schemas/idl
Content-Type: application/json

{
"name": "spi",
"type": "idl",
"file": "file:///path/to/your/spi.zip"  // 请替换为实际的 spi.zip 文件路径
}

```

**`spi.zip` 文件结构示例：**

当 IDL schema 包含多个相互依赖的文件或需要辅助资源时，使用 ZIP 归档能够提供便捷的打包和分发机制。`spi.zip` 归档内部应包含一个主 schema 文件，并可选择包含一个与 schema 同名（不含扩展名）的目录，用于存放辅助文件。例如：

```

spi.zip/
├── spi.idl          // 主 IDL schema 文件
└── spi/             // 可选的辅助文件目录
├── common.idl
└── definitions.txt

````

### 2. 创建使用 IDL Schema 的流

在 eKuiper 环境中，流的定义用于明确数据源及其内在结构。当集成 IDL schema 时，用户可在流定义中明确引用已创建的 IDL schema，从而使 eKuiper 能够精确解析传入的二进制数据。

**创建方法：**

流的创建通过向 eKuiper 的 `/streams` 端点发送 `POST` 请求完成。请求体中包含流的 JSON 定义，其中 `schemaId` 字段用于关联先前创建的 IDL schema。

**`spiStream.json` 内容示例：**

以下是 `spiStream.json` 文件的内容示例，该文件定义了一个名为 `spiStream` 的流：

```json
{
  "sql": "CREATE STREAM spiStream() WITH (TYPE=\"mqtt\",FORMAT=\"spi\",DATASOURCE=\"canspi\",SCHEMAID=\"spi\", SHARED=\"true\")"
}
````

* `format`: 此字段强制设置为 `BINARY`，因为 IDL schema 的核心功能即在于解析非结构化的原始字节数据流。

* `schemaId`: 此字段用于引用通过 `/schemas/idl` API 创建的 IDL schema 的名称（在本示例中为 `spi`）。

* `SHARED="true"`: 此选项指示 MQTT 订阅将作为共享订阅处理，允许多个消费者从同一订阅中接收消息，从而实现负载均衡。

**REST API 请求示例：**

```
POST http://localhost:9020/streams
Content-Type: application/json

{
  "sql": "CREATE STREAM spiStream() WITH (TYPE=\"mqtt\",FORMAT=\"spi\",DATASOURCE=\"canspi\",SCHEMAID=\"spi\", SHARED=\"true\")"
}
```

### 3. 创建规则并处理数据

流定义完成后，用户可着手构建规则以对流中的数据进行查询与处理。eKuiper 系统将自动运用 IDL schema 解析传入的二进制数据，使其在 SQL 规则表达式中可被有效访问与操作。

**创建方法：**

规则的创建通过向 eKuiper 的 `/rules` 端点发送 `POST` 请求实现。请求体中包含规则的 JSON 定义，该规则将从已关联 IDL schema 的流中提取数据，并将其定向至指定的输出目标（例如 MQTT 主题）。

**`spiRule.json` 内容示例：**

以下是 `spiRule.json` 文件的内容示例，该文件定义了一个名为 `spi1` 的规则：

```json
{
  "id": "spi1",
  "name": "Read SPI Data",
  "sql": "SELECT ts, `ZCUDZCUCANFD2Fr36$BswAppVersion`, `Mess0$Mess0_Sig2` FROM spiStream",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "ek/result1",
        "qos": 1,
        "format": "json",
        "sendSingle": true
      }
    }
  ]
}
```

* `sql`: 此字段包含用于从流中筛选和转换数据的 SQL 查询语句。值得注意的是，SQL 查询中引用的字段（例如 `ts`、`ZCUDZCUCANFD2Fr36$BswAppVersion` 和 `Mess0$Mess0_Sig2`）是根据所关联的 IDL schema 对原始二进制数据进行解析后自动生成的。

**REST API 请求示例：**

```
POST http://localhost:9020/rules
Content-Type: application/json

{
  "id": "spi1",
  "name": "Read SPI Data",
  "sql": "SELECT ts, `ZCUDZCUCANFD2Fr36$BswAppVersion`, `Mess0$Mess0_Sig2` FROM spiStream",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "ek/result1",
        "qos": 1,
        "format": "json",
        "sendSingle": true
      }
    }
  ]
}
```

### 4. 发布数据与验证结果

一旦规则部署就绪，用户即可向流定义中指定的数据源（例如 MQTT 主题 `canspi`）发布原始二进制数据。eKuiper 系统将自动运用 `spi` IDL schema 对这些数据进行解析，并依据既定规则执行处理。

**数据发布：**

请确保发布的原始二进制数据严格符合 IDL schema 中定义的格式，并将其发送至流定义中指定的 MQTT 主题（例如 `canspi`）。

**预期结果验证：**

订阅规则中定义的输出主题（例如 `ek/result1`）。您将接收到由 eKuiper 处理并解析后的结构化 JSON 数据。通过审视这些 JSON 数据，可以验证 IDL schema 是否已成功将原始二进制数据转换为可读格式。此外，您还可以通过查询 eKuiper 的 `/rules/{ruleId}/status` API 来监控规则的运行状态和处理指标。

**REST API 获取规则状态示例：**

```
GET http://localhost:9020/rules/spi1/status
```

综上所述，通过遵循上述步骤，用户能够在 eKuiper 平台中高效地利用 IDL schema 处理和解析复杂的二进制数据流。

## 数据格式

以下文档通过示例详细描述了如何利用 IDL (Interface Description Language) schema 定义数据结构，并将其应用于解析原始二进制数据。

-----

## 基于 IDL 定义解析二进制数据

在物联网和工业控制领域，设备常常以原始二进制格式传输数据，这些数据通常缺乏明确的结构定义，难以直接解读和处理。Interface Description Language (IDL) Schema 提供了一种标准化的方式来精确描述这些二进制数据流的内部结构。eKuiper 作为一个轻量级物联网边缘流式数据处理引擎，能够利用 IDL Schema 对这些原始二进制数据进行解析，将其转化为结构化、可查询的格式，从而简化后续的数据分析和应用集成。

本示例将结合 `spi.idl` 和 `spi.lines` 文件，阐述 IDL 如何作为二进制数据的“蓝图”，指导 eKuiper 进行数据解析。

### IDL Schema 结构详解

SPI 数据包聚合了多个 CAN 帧（frame）。`spi.idl` 文件定义了数据包 (`packet`) 和数据帧 (`frame`) 的结构。

```idl
module spi {
    struct frame {
        unsigned long id; [cite_start]// 4 字节 id [cite: 1]
        unsigned long len; [cite_start]// 4 字节 长度 [cite: 2, 3]
        sequence<octet> payload;  [cite_start]// len字节Payload [cite: 1]
    };
    struct packet {
        unsigned long long ts; [cite_start]// 8字节 时间戳 [cite: 4, 5]
        unsigned short len; [cite_start]// 2字节 总长度 [cite: 4, 6]
        [cite_start]@format(dbc="spi/sim.json") sequence<frame> frames; [cite: 4]
    }
}
```

* **`module spi`**: 定义了一个名为 `spi` 的模块，封装了相关的结构体定义。
* **`struct packet`**: 描述了完整数据包的结构，包含一个时间戳、总长度以及多个数据帧的序列。
   * `unsigned long long ts`: 表示数据包的时间戳，占用 8 个字节。
   * `unsigned short len`: 表示整个 `frames` 序列的实际总长度（以字节计），占用 2 个字节。
   * `@format(dbc="spi/sim.json") sequence<frame> frames`: 这是数据包的核心部分，表示一个 `frame` 结构体的序列（即数组）。其长度可变，运行时读取 len 可知其长度。
* **`struct frame`**: 描述了单个 CAN 数据帧的结构。
   * `unsigned long id`: 表示帧的标识符，占用 4 个字节。
   * `unsigned long len`: 表示帧中 `payload` 的长度，占用 4 个字节。
   * `sequence<octet> payload`: 表示帧的实际数据 paylaod，其长度由 `len` 字段动态指定。

### 示例原始二进制数据

通过 MQTT 发送的一条二进制数据代表一个一个独立的 `packet`。以下为一个示例 packet 数据。

```
000001931a873e9f0058000004F000000040FFFFFFFFFFFFFFFF000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000586000000081111111111111111
```

其拼接方式如下所示：

```
000001931a873e9f     8字节 时间戳（毫秒，uint64）
0058                 2字节 frames 长度，此处为 88
// 以下 88 位为具体的 CAN 帧
000004F0             4字节  CAN Id(uint32)
00000040             4字节  帧长度（payload长度），此处为 64，应当为 CAN FD 帧
FFFFFFFFFFFFFFFF0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
                     64字节  CAN 帧 payload
// 下一帧
000000586    id
00000008     len
1111111111111111   payload
```
