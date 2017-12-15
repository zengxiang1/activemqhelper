package org.zx.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zx 17-12-15
 */
public class ActiveMQ {
    private static Map<String, ActiveMQ> activeMQMap = new HashMap<>();
    private ActiveMQConnectionFactory activeMQConnectionFactory;
    private Session session;
    private Connection connection;

    private static final Logger log = LoggerFactory.getLogger(ActiveMQ.class);

    public ActiveMQ(ActiveMQConnectionFactory activeMQConnectionFactory) {
        this.activeMQConnectionFactory = activeMQConnectionFactory;
    }
    private ActiveMQ(){

    }

    public static ActiveMQ getInstance(String userName, String password, String brokerUrl){

        if (activeMQMap.containsKey(brokerUrl)){
            return activeMQMap.get(brokerUrl);
        }
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(userName, password, brokerUrl);
        ActiveMQ activeMQ = new ActiveMQ(activeMQConnectionFactory);
        try {
            activeMQ.connection = activeMQConnectionFactory.createConnection();
            activeMQ.session = activeMQ.connection.createSession(true, Session.SESSION_TRANSACTED);
        } catch (JMSException e) {
            e.printStackTrace();
            log.error("获取链接错误: broker{}",brokerUrl);
        }
        activeMQMap.put(brokerUrl, activeMQ);
        return activeMQ;
    }

    public void sendTextMessage(String destinationStr, String message){
        try {
            Queue queue = session.createQueue(destinationStr);
            MessageProducer messageProducer = session.createProducer(queue);
            TextMessage textMessage = session.createTextMessage(message);
            messageProducer.send(textMessage);
            session.commit();
        }catch (JMSException e){
            e.printStackTrace();
            log.info("发送消息失败:{}",message);
        }
    }

    public void sendTextMessage(String destinationStr, String message, long delay){
        try {
            Queue queue = session.createQueue(destinationStr);

            MessageProducer messageProducer = session.createProducer(queue);
            TextMessage textMessage = session.createTextMessage(message);
            textMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
            messageProducer.send(textMessage);
            session.commit();
        }catch (JMSException e){
            e.printStackTrace();
            log.info("发送消息失败:{}",message);
        }
    }

    public void sendTextMessage(String destinationStr, String message, long delay, long period,int repeat){
        try {
            Queue queue = session.createQueue(destinationStr);
            MessageProducer messageProducer = session.createProducer(queue);
            TextMessage textMessage = session.createTextMessage(message);
            textMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
            textMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, period);
            textMessage.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, repeat);
            messageProducer.send(textMessage);
            session.commit();
        }catch (JMSException e){
            e.printStackTrace();
            log.info("发送消息失败:{}",message);
        }
    }


}
