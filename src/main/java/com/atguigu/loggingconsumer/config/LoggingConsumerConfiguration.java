package com.atguigu.loggingconsumer.config;

import com.rabbitmq.client.Channel;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@Configuration
public class LoggingConsumerConfiguration {

    /**
     * 生产者端动态路由
     */
    @Bean
    public Function<String, Message<String>> destinationAsPayload() {
        return value -> MessageBuilder.withPayload(value)
                .setHeader("spring.cloud.stream.sendto.destination", value).build();
    }

    /**
     * 在spring cloud stream 3.0版本中,消费者有2种类型:
     *      一. 消息驱动(异步): 只要有消息到达, 就立即消费. 此处使用的是普通的异步模式.
     *      二. 轮询(类似于同步概念, polled): 如果希望控制消息的处理速率，则可能需要使用同步的消费者模式
     *  对于反应式消费者, 还支持Tuple类型(元组类型, 例如Tuple2)
     * @return 消费者
     */
    @Bean
    public Consumer<Message<Person>> logPerson() {
        return message -> {
                System.out.println("Received: " + message.getPayload());
                MessageHeaders headers = message.getHeaders();
                System.out.println(headers);
                System.out.println(headers.get(AmqpHeaders.DELAY, Long.class));
                // TODO 注意业务代码要用try-catch包起来,防止异常导致ACK失败, 卡死mq
                if (headers.containsKey(AmqpHeaders.CHANNEL)){
                    // 暂定为手动ack
                   Channel channel = headers.get(AmqpHeaders.CHANNEL, Channel.class);
                   Long deliverTag = headers.get(AmqpHeaders.DELIVERY_TAG, Long.class);
                    assert channel != null;
                    assert  deliverTag != null;
                    try {
                        channel.basicAck(deliverTag, false);
                        System.out.println("手动ack成功...");
                    } catch (IOException e) {
                        log.error("[logPerson consumer] 消费异常, errorMessage={}", e.getMessage(), e);
                    }
                }

//            assert accessor != null;
//            Long deliveryTag = accessor.getDeliveryTag();
//            System.err.println("deliveryTag: " + deliveryTag);
            // 仅能用于Polled Consumers
            // Objects.requireNonNull(StaticMessageHeaderAccessor.getAcknowledgmentCallback(message)).acknowledge();
            // 测试消息失败时的错误处理: DLQ
            // throw new RuntimeException("throw exception!");
        };
    }

    /**
     * 使用Supplier生成的source
     * warning:
     *      从3.0版本开始, springCloudStream不再单独提供reactor类型的框架包, 而是一个框架, 同时支持命令式和反应式.
     *      对于反应式生产者, @Bean注解标注下, 框架会自动识别区分, 并且只调用一次,(因为调用它的get（）方法会生成（提供）连续的消息流，而不是单个消息);
     *      然而，对于有限流, 鉴于其所产生流的有限性，此类供应商仍然需要定期调用。此时必须使用 @PollableBean
     *      `@PollableBean`注解中有个属性: splittable, 默认为true, 框架将把返回的每个条目作为单独的消息发送出去。
     *      如果不希望如此, 则可以设置为false, 这样会直接返回所生产的响应流
     * @return 生产者
     */
    @Bean
    public Supplier<Message<Person>> emitPerson() {
        return () -> {
            Person person = new Person();
            person.setName(UUID.randomUUID().toString());
            person.setId((int) (Math.random() * 100));
            person.setDelayTime(6500L);
            System.err.println("Emit: " + person);
            return MessageBuilder.withPayload(person).setHeader("x-delay", 6500L).build();
        };
    }

    @Data
    public static class Person {
        /**
         * 姓名
         */
        private String name;
        /**
         * id
         */
        private Integer id;
        /**
         * 延迟毫秒数
         */
        private long delayTime;
    }
}
