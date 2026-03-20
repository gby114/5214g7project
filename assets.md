### 数据

```
realtime -> temp1_raw_data -> kafka(topic: polymarket.topic1) -> agg_temp1_hour_data -> agg_temp1_day_data
historical -> temp2_raw_data -> kafka(topic: polymarket.topic2) -> agg_temp2_hour_data -> agg_temp2_day_data
```

