package com.example.demo.producer;

import com.example.demo.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
@Log4j2
public class Producer {
    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void sendLibraryEvent (LibraryEvent libraryEvent) throws JsonProcessingException {
        final Integer key = libraryEvent.getLibraryEventId();
        final String value = objectMapper.writeValueAsString(libraryEvent);
        //  kafkaTemplate.send() if we specified topics then
        //if we specified kafkaTemplate.sendDefault() topic in yml
       // kafkaTemplate.send("",2,""); topic,key,value
        final ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.sendDefault(key, value);
        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {  //ListenableFutureCallback has 2 abstract method onFailure and onSuccess
            @Override
            public void onFailure (Throwable throwable) {
                handleFailure(key, value);
            }

            @Override
            public void onSuccess (SendResult<Integer, String> integerStringSendResult) {
                handleSuccess(key, value, integerStringSendResult);
            }
        });
    }

    private void handleFailure (Integer key, String value) {
        log.error("Error sending the message key{}, and value {}", key, value);
    }

    private void handleSuccess (Integer key, String value, SendResult<Integer, String> integerStringSendResult) {
        log.info("Message sent successfully {} and value: {}, partition is {}", key, value, integerStringSendResult.getRecordMetadata().partition());
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous (LibraryEvent libraryEvent) throws JsonProcessingException {
        final Integer key = libraryEvent.getLibraryEventId();
        final String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;
        try {    //call default topic only
            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error InterruptedException");
        } catch (Exception e) {
            log.error("Error Exception");
        }
        return sendResult;
    }

    public void sendLibraryEvent3rd (LibraryEvent libraryEvent) throws JsonProcessingException {
        final String value = objectMapper.writeValueAsString(libraryEvent);
        //this approach we can call any topics
        kafkaTemplate.send("library-events", libraryEvent.getLibraryEventId(), value);
    }


}
