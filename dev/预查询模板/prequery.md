
# 1.请求创建预查询模板
## 接口地址
`localhost:8081/druid/indexer/v1/datasources/dwm_request/createPreQueryTemplate`
## 请求方式
`POST`
## 请求参数
Header：Content-Type=application/json

Body：
```json
{
  "dataSource": "dwm_request",
  "intervalStr": "2019-01-23T/2019-01-24T", //多个时段，英文逗号分隔
  "dimensions": ["appsysid","method",
    "status","status_code"],
  "metrics": ["dur_sum","dur_max","dur_min"],
  "lifeTime": 22,  // 预查询模板有效期，0-23 hour
  "queryGranularity": {
    "type": "period",
    "period": "PT10M"
  }
}
```

## 示例1
### body
```json
{
  "dataSource": "dwm_request",
  "intervalStr": "2019-01-21T/2019-01-23T", //多个时段，英文逗号分隔
  "dimensions": ["type", "group", "appid", "appsysid", "agent",
    "path", "method", "pappid", "pappsysid",
    "status","status_code"],
  "metrics": ["dur_sum","dur_max","dur_min"],
  "lifeTime": 18,  // 预查询模板有效期，0-23 hour
  "queryGranularity": {
    "type": "period",
    "period": "PT5M" 
  }
}
```

# 2.预查询任务创建
## 接口地址

## 请求方式

## 请求参数
Header：Content-Type=application/json

Body：
```json
{
  "type": "PreQuery",
  "preQueryTaskConfig": {
    "skipCheckTemplateLifeTime": true
  },
  "inputDataSourceSpec":{
    "dataSource":"dwm_request",
    "dimensionsSpec":{
      "dimensions": [
        "type", "group", "appid", "appsysid", "agent",
        "path", "method", "root_appid", "pappid", "pappsysid",
        "pagent", "pagent_ip", "uevent_model", "uevent_id", "user_id", "session_id",
        "host", "ip_addr", "province", "city", "page_id", "page_group", "tag","service_type","papp_type",
        "status","status_code"
      ]
    },
    "metricsSpec":[
      {
        "type": "count",
        "name": "count"
      },
      {
        "type": "longSum",
        "name": "dur_sum",
        "fieldName": "dur_sum"
      },
      {
        "type": "longMin",
        "name": "dur_min",
        "fieldName": "dur_min"
      },
      {
        "type": "longMax",
        "name": "dur_max",
        "fieldName": "dur_max"
      },
      {
        "type": "longSum",
        "name": "biz_sum",
        "fieldName": "biz_sum"
      },
      {
        "type": "longMin",
        "name": "biz_min",
        "fieldName": "biz_min"
      },
      {
        "type": "longMax",
        "name": "biz_max",
        "fieldName": "biz_max"
      },
      {
        "type": "longSum",
        "name": "fail_sum",
        "fieldName": "fail_sum"
      },
      {
        "type": "longMin",
        "name": "fail_min",
        "fieldName": "fail_min"
      },
      {
        "type": "longMax",
        "name": "fail_max",
        "fieldName": "fail_max"
      },
      {
        "type": "longSum",
        "name": "httperr_sum",
        "fieldName": "httperr_sum"
      },
      {
        "type": "longMin",
        "name": "httperr_min",
        "fieldName": "httperr_min"
      },
      {
        "type": "longMax",
        "name": "httperr_max",
        "fieldName": "httperr_max"
      },
      {
        "type": "longSum",
        "name": "neterr_sum",
        "fieldName": "neterr_sum"
      },
      {
        "type": "longMin",
        "name": "neterr_min",
        "fieldName": "neterr_min"
      },
      {
        "type": "longMax",
        "name": "neterr_max",
        "fieldName": "neterr_max"
      },
      {
        "type": "longSum",
        "name": "err_sum",
        "fieldName": "err_sum"
      },
      {
        "type": "longMin",
        "name": "err_min",
        "fieldName": "err_min"
      },
      {
        "type": "longMax",
        "name": "err_max",
        "fieldName": "err_max"
      },
      {
        "type": "longSum",
        "name": "tolerated_sum",
        "fieldName": "tolerated_sum"
      },
      {
        "type": "longMin",
        "name": "tolerated_min",
        "fieldName": "tolerated_min"
      },
      {
        "type": "longMax",
        "name": "tolerated_max",
        "fieldName": "tolerated_max"
      },
      {
        "type": "longSum",
        "name": "frustrated_sum",
        "fieldName": "frustrated_sum"
      },
      {
        "type": "longMin",
        "name": "frustrated_min",
        "fieldName": "frustrated_min"
      },
      {
        "type": "longMax",
        "name": "frustrated_max",
        "fieldName": "frustrated_max"
      },
      {
        "type": "longSum",
        "name": "exception_sum",
        "fieldName": "exception_sum"
      },
      {
        "type": "longMin",
        "name": "exception_min",
        "fieldName": "exception_min"
      },
      {
        "type": "longMax",
        "name": "exception_max",
        "fieldName": "exception_max"
      },
      {
        "type": "longSum",
        "name": "err_4xx_sum",
        "fieldName": "err_4xx_sum"
      },
      {
        "type": "longMin",
        "name": "err_4xx_min",
        "fieldName": "err_4xx_min"
      },
      {
        "type": "longMax",
        "name": "err_4xx_max",
        "fieldName": "err_4xx_max"
      },
      {
        "type": "longSum",
        "name": "err_5xx_sum",
        "fieldName": "err_5xx_sum"
      },
      {
        "type": "longMin",
        "name": "err_5xx_min",
        "fieldName": "err_5xx_min"
      },
      {
        "type": "longMax",
        "name": "err_5xx_max",
        "fieldName": "err_5xx_max"
      }
    ],
    "granularitySpec": {
      "type": "uniform",
      "segmentGranularity": "HOUR",
      "queryGranularity": "NONE"
   }
  },
  "tuningConfig": {
    "type": "index_parallel"
  },
  "context": {
    "maxTaskCount": 1
  }
}
```

# 3.转发预测查询窗口向量接口
## 3.1.接口说明
### 3.1.1.接口地址
http://127.0.0.1:6666/predict
### 3.1.2.请求方式
POST
### 3.1.3.请求参数
Header Content-Type: application/json

Body:示例
```json
{
    "input":"10000100000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000011110000000000000000001000000000000000000010000000000000100011000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000110000000000000000000000100000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000111100000000000000"
}
