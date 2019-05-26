# 基于kafkaStream对招聘数据清洗

##简介：
    
    由于爬虫对招聘数据抓取下来，格式五花八门，因此需要对抓取下来的字段进行简单清洗，
    首先在抓取部分将数据直接发送至kafka，然后我们便可借助kafkaStream对抓取数据
    进行实时清洗，最终落入MongoDB。

## kafkaStream简介：
    [Kfaka Stream使用总结]()
    [kafka教程文档]()
