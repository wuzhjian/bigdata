package com.monitor.bigdata.controller;

import com.monitor.bigdata.model.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

/**
 * @author 44644
 * kafka 监控指标
 */
@RestController
public class KafkaDataProviderController {

    protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private static final String MESSAGE_IN_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";
    private static final String BYTES_IN_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec";
    private static final String BYTES_OUT_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec";
    private static final String PRODUCE_REQUEST_PER_SEC = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce";
    private static final String CONSUMER_REQUEST_PER_SEC = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchConsumer";
    private static final String FLOWER_REQUEST_PER_SEC = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchFollower";
    private static final String ACTIVE_CONTROLLER_COUNT = "kafka.controller:type=KafkaController,name=ActiveControllerCount";
    private static final String PART_COUNT = "kafka.server:type=ReplicaManager,name=PartitionCount";



    @RequestMapping(value = "/monitor", method = RequestMethod.GET)
    public Result extractMonitorData() throws MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException, IOException {
        //TODO 通过调用API获得IP以及参数
        String jmxURL = "service:jmx:rmi:///jndi/rmi://192.168.1.26:9988/jmxrmi";
        System.out.println(jmxURL);
        JMXServiceURL url = new JMXServiceURL(jmxURL);
        JMXConnector jmxConnector = JMXConnectorFactory.connect(url);

        MBeanServerConnection jmxConnection = jmxConnector.getMBeanServerConnection();
        LOGGER.info("jmxConnector connect success");
        ObjectName messageCountObj = new ObjectName(MESSAGE_IN_PER_SEC);
        ObjectName bytesInPerSecObj = new ObjectName(BYTES_IN_PER_SEC);
        ObjectName bytesOutPerSecObj = new ObjectName(BYTES_OUT_PER_SEC);
        ObjectName produceRequestsPerSecObj = new ObjectName(PRODUCE_REQUEST_PER_SEC);
        ObjectName consumerRequestsPerSecObj = new ObjectName(CONSUMER_REQUEST_PER_SEC);
        ObjectName flowerRequestsPerSecObj = new ObjectName(FLOWER_REQUEST_PER_SEC);
        ObjectName activeControllerCountObj = new ObjectName(ACTIVE_CONTROLLER_COUNT);
        ObjectName partCountObj = new ObjectName(PART_COUNT);


        Long messagesInPerSec = (Long) jmxConnection.getAttribute(messageCountObj, "Count");
        Long bytesInPerSec = (Long) jmxConnection.getAttribute(bytesInPerSecObj, "Count");
        Long bytesOutPerSec = (Long) jmxConnection.getAttribute(bytesOutPerSecObj, "Count");
        Long produceRequestCountPerSec = (Long) jmxConnection.getAttribute(produceRequestsPerSecObj, "Count");
        Long consumerRequestCountPerSec = (Long) jmxConnection.getAttribute(consumerRequestsPerSecObj, "Count");
        Long flowerRequestCountPerSec = (Long) jmxConnection.getAttribute(flowerRequestsPerSecObj, "Count");
        Integer activeControllerCount = (Integer) jmxConnection.getAttribute(activeControllerCountObj, "Value");


        System.out.println("messagesInPerSec:  " + messagesInPerSec);
        System.out.println("bytesInPerSec:  " + bytesInPerSec);
        System.out.println("bytesOutPerSec:  " + bytesOutPerSec);
        System.out.println("produceRequestCountPerSec:  " + produceRequestCountPerSec);
        System.out.println("consumerRequestCountPerSec:  " + consumerRequestCountPerSec);
        System.out.println("flowerRequestCountPerSec:  " + flowerRequestCountPerSec);
        System.out.println("activeControllerCount:  " + activeControllerCount);

        return Result.ofMessage(500, "www");
    }

}
