package com.imitee.mqtt_demo;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * @author: luo
 * @create: 2020-04-27 21:58
 **/
public class Main {

    public static final String HOST = "xxxxx";

    public static void main(String[] args) {

        new Thread() {
            @Override
            public void run() {
                try {
                    MQTT mqtt = new MQTT();

                    mqtt.setHost(HOST, 1883);

                    //client.will_set(topic="agvlocate", payload="我掉线了", qos=0, retain=True)
                    mqtt.setWillTopic("agvlocate");
                    mqtt.setWillQos(QoS.AT_MOST_ONCE);
                    mqtt.setWillMessage("我掉线了");
                    mqtt.setWillRetain(false);

                    //使用同步阻塞模式
                    BlockingConnection connection = mqtt.blockingConnection();

                    connection.connect();

                    while (true){

                        Thread.sleep(2000);

                        String data = "你好"+System.currentTimeMillis();

                        connection.publish("agvlocate", data.getBytes(), QoS.AT_LEAST_ONCE, false);
                    }


                    //connection.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                try {
                    MQTT mqtt = new MQTT();

                    mqtt.setHost(HOST, 1883);

                    //client.will_set(topic="agvlocate", payload="我掉线了", qos=0, retain=True)
                    mqtt.setWillTopic("agvlocate");
                    mqtt.setWillQos(QoS.AT_MOST_ONCE);
                    mqtt.setWillMessage("我掉线了");
                    mqtt.setWillRetain(false);

                    //使用同步阻塞模式
                    BlockingConnection connection = mqtt.blockingConnection();

                    connection.connect();
                    Topic[] topics = {new Topic("agvlocate", QoS.AT_LEAST_ONCE)};
                    connection.subscribe(topics);

                    while (true){
                        Message message = connection.receive();
                        byte[] payload = message.getPayload();

                        System.out.printf("收到： topic=%s,message=%s \n",message.getTopic(),new String(payload));

                        message.ack();
                    }


                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        while (true);

    }
}
