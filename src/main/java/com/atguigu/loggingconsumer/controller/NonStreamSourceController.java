package com.atguigu.loggingconsumer.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/message")
public class NonStreamSourceController {

    /**
     * 命令式编程时手动发送数据用
     * reactive使用: EmitterProcessor
     */
    @Autowired
    private StreamBridge streamBridge;

    @PostMapping("/send")
    public String delegateToSupplier(@RequestBody String body) {
        System.out.println("Sending " + body);
        // GenericMessage<String> message = new GenericMessage<>(body);
        boolean sendSucceed = streamBridge.send("toStream-out-0", body);
        return sendSucceed ? "success" : "fail";
    }
}
