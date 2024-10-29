# 轮转帧测试

确保 eKuiper 已替换，geely_api 已启动。

## 电压测试

0. MQTTX 订阅车端主题 result/#，等待查看结果
1. 创建电压流：
   ```
   curl -X POST --location "http://127.0.0.1:9081/streams" -H "Content-Type: application/json" -d '{"sql": "CREATE STREAM celluStream() WITH (TYPE=\"mqtt\",FORMAT=\"cellu\",DATASOURCE=\"batt/cell\", SHARED=\"true\");"}'
   ```
2. 创建电压采集规则：
   ```
   curl -X POST --location "http://127.0.0.1:9081/rules" -H "Content-Type: application/json" -d '{"id": "ruleCellu","name": "电池电压收集","sql": "SELECT event_time() as ts, collectu(hvBattCellUInfo_UB, hvBattCellUInfoU1, hvBattCellUInfoU2, hvBattCellUInfoU3, hvBattCellUInfoU4) as data FROM celluStream WHERE IsNull(data) = false","actions": [{"mqtt": {"server": "tcp://127.0.0.1:1883","topic": "result/{{.ts}}/ruleCellu","format": "delimited","fields": ["data"],"hasHeader": false,"sendSingle": true}}]}'
   ```
3. 查看规则状态，确保有数据输入输出。然后查看 MQTTX 订阅结果
   ```
   curl -X GET --location "http://127.0.0.1:9081/rules/ruleCellu/status"
   ```

## 电池温度测试

0. MQTTX 订阅车端主题 result/#，等待查看结果
1. 创建电池温度流：
   ```
   curl -X POST --location "http://127.0.0.1:9081/streams" -H "Content-Type: application/json" -d '{"sql": "CREATE STREAM celltStream() WITH (TYPE=\"mqtt\",FORMAT=\"cellt\",DATASOURCE=\"batt/tsnsr\", SHARED=\"true\");"}'
   ```
2. 创建电压采集规则：
   ```
   curl -X POST --location "http://127.0.0.1:9081/rules" -H "Content-Type: application/json" -d '{"id": "ruleCellt","name": "电池温度收集","sql": "SELECT event_time() as ts, collectt(HvBattCellTInfoHvBattTSnsrNr, HvBattCellTInfoHvBattSnsrT) as data FROM celltStream WHERE IsNull(data) = false","actions": [{"mqtt": {"server": "tcp://127.0.0.1:1883","topic": "result/{{.ts}}/ruleCellt","format": "delimited","fields": ["data"],"hasHeader": false,"sendSingle": true}}]}'
   ```
3. 查看规则状态，确保有数据输入输出。然后查看 MQTTX 订阅结果
   ```
   curl -X GET --location "http://127.0.0.1:9081/rules/ruleCellu/status"
   ```