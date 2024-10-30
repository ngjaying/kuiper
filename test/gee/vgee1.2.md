# Gee 测试

前提条件：

1. 确保 kuiperd 二进制已替换
2. 更改/确保 etc/kuiper.yaml 中的 aesKey 更新为 `YTA5NThiYTAyMTRkNmZhNg==`
3. 确保启动eKuiper时已传递如下环境变量，可查看启动log（需要新版sdvflow)，分别为配置 vin，车系 cs，国家 nc
    ```
    KUIPER_PROPS_V=L6T78CNW6RY000039
    KUIPER_PROPS_CS=a4
    KUIPER_PROPS_NC=cn
    ```

## 导入规则

提供两个文件：

- min1.2.json: 只包含最少要测的规则，包括周期，单个事件规则（设置成40s必触发），历史，轮转帧*2
- rules1.2.json: 包含所有已定义规则

注意规则参数：

- mqtt 地址
- dbc 地址
- 文件保存位置

```
curl -X POST --location "http://127.0.0.1:9081/data/import?partial=1" -H "Content-Type: application/json" -d '{"file": "file:///mnt/c/temp/min1.2.json"}'
```

## 结果

### 全量周期采集

topic:
$file/periodic/rule10secondCSV_ZSTD/a4/cn/19216822121/1.0/1730259207994/60e2a8da304fe062e77b54c4b779a359/mock-1730259200159.zstd

### 轮转帧采集

- 电压采集范例
  Topic: result/cell/1/ruleCellu/EX11/Malaysia/L6T78CNW6RY000039/1.0/1730271599316
  hvBattCellUInfoU1,hvBattCellUInfoU2,hvBattCellUInfoU3,hvBattCellUInfoU4
  3,5,1,26544
  4,5,3,31320

N9UsTj7Kz/Ql3CH+VFFD5gAAAAAAAAAAAAAAAAAAAAB949krmzgr0cWrraKxmXr2+fcoDqV+IqEMkrZcfLeTIdpQV5wnc3y/5efPJELnah0hYu4sei0T49+G8Xci1A95vow8ctI=

- 温度采集范例

Topic: result/cell/2/ruleCellt/EX11/Malaysia/L6T78CNW6RY000039/1.0/1730271704501QoS: 0
HvBattCellTInfoHvBattTSnsrNr,HvBattCellTInfoHvBattSnsrT
1,34
2,34
3,35

实际为压缩加密模式
w+RR5hjqZrM9rIiUYyELigAAAAAAAAAAAAAAAAAAAAB949krmzhD0cXLrYKxmXr2+fcoDqV/IqEMkrc+PvbSW6JPKMd4MASH4v7IJELhahs5W/EzeCsj05zru8QwHiRae1E=

### 事件采集

上传topic: result/event/rulePick2/a4/cn/19216822121/1730255390056
即：result/event/{{规则id}}/{{车型}}/{{地域}}/{{Vin码}}/{{事件时间}}

上传payload为 csv + zstd 压缩 + aes 加密

### 历史采集

topic:
$file/history/ruleHistoryQuery1/a4/cn/19216822121/1.0/1730259207994/60e2a8da304fe062e77b54c4b779a359/ruleHistoryQuery1-1730259200159.zstd
