package com.example.demo.controller;

import com.example.demo.domain.LibraryEvent;
import com.example.demo.producer.Producer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Log4j2
@RestController
@RequestMapping("/event")
public class LibraryEvenController {
    @Autowired
   private  Producer producer;
    @PostMapping(value = "/v1/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        producer.sendLibraryEvent(libraryEvent);
        final SendResult<Integer, String> sendResult = producer.sendLibraryEventSynchronous(libraryEvent);
        log.info("sendResult: "+sendResult);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
