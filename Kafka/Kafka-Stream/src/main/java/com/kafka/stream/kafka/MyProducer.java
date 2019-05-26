package com.kafka.stream.kafka;


import com.kafka.stream.util.PropertyReaderUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

/**
 * @author haojiewu
 * @desc
 * @create 18-4-18
 */
public class MyProducer {
    static PropertyReaderUtil reader = new PropertyReaderUtil();
    public static void main(String[] args) {
        Map<String, String> serverMap = reader.readPropertyFile("kafkaServer.properties");
        Properties props = new Properties();
//        props.put("bootstrap.servers", "yun1.kafka.zcbigdata.com:29092,yun2.kafka.zcbigdata.com:29092,yun3.kafka.zcbigdata.com:29092");
        props.put("bootstrap.servers", "cluster1.kafka.zcbigdata.com:9090,cluster2.kafka.zcbigdata.com:9091,cluster3.kafka.zcbigdata.com:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String value = "{'name':'jack'}";
        String value1 = "{\"params\":[\"王继\",\"132527197512248510\"],\"results\":[{\"caseCode\":\"（2018）冀0730执恢80号\",\"entityName\":\"王继海\"},{\"caseCode\":\"（2018）冀0730执23号\",\"entityName\":\"王继海\"}]}";
        String value2 = "{\"cityAddress\":\"深圳 \",\"companyAddress\":\"['深圳南山区创业路中兴工业城8栋2楼', '深圳南山区东滨路4078号永新汇3号楼']\",\"companyName\":\"深圳市环球易购电子商务有限公司\",\"educationMessage\":\"学历不限\",\"fringeBenefits\":\"跨境电商,龙头企业,发展前景好\",\"industryItem\":\"{'firstItem': '金融', 'secondItem': '投融资', 'thirdItem': '并购'}\",\"positionName\":\"系统运维\",\"positionUrl\":\"https://www.lagou.com/jobs/3012504.html\",\"salaryPrice\":\"5k-10k\",\"source\":\"拉勾网\",\"workAddress\":\"深圳 - 南山区 - 深圳市南山区创业路中兴工业城8栋2楼\",\"workBackground\":\"经验3-5年\",\"publishedDate\":\"1天前 \",\"email\":\"[]\",\"phoneNum\":\"[]\",\"companyUrl\":\"www.lgstatic.com/thumbnail_160x160/image1/M00/38/9F/CgYXBlWmIt6Af8k5AABvK21LZWM490.jpg?cc=0.211507520172745\",\"addressUrl\":\"https://www.lagou.com/gongsi/83025.html\"}";
//        IntStream.range(0,200).forEach(p->producer.send(new ProducerRecord<>("demo", "test", value)));
//        IntStream.range(0,300).forEach(p->producer.send(new ProducerRecord<>("demo", "test", value1)));
        producer.send(new ProducerRecord<>("demo",value1));
        producer.close();
    }
}
