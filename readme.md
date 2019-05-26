# 基于kafkaStream对招聘数据清洗

##简介：
    
    由于爬虫对招聘数据抓取下来，格式五花八门，因此需要对抓取下来的字段进行简单清洗，
    首先在抓取部分将数据直接发送至kafka，然后我们便可借助kafkaStream对抓取数据
    进行实时清洗，最终落入MongoDB。

## kafkaStream简介：
1. [Kfaka Stream使用总结](https://github.com/Liangchengdeye/kafkaStream/blob/master/Kfaka%20Stream%E4%BD%BF%E7%94%A8%E6%80%BB%E7%BB%93.pdf)
2. [kafka教程文档](https://github.com/Liangchengdeye/kafkaStream/blob/master/Kafka/%E6%96%87%E6%A1%A3/kafka%E6%95%99%E7%A8%8B%E6%96%87%E6%A1%A3.pdf)
