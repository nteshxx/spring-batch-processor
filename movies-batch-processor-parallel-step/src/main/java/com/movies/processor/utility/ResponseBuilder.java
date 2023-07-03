package com.movies.processor.utility;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class ResponseBuilder {
    
    public static ResponseEntity<Object> build(HttpStatus httpStatus, String message, String error, Object data) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("timestamp", Instant.now());
        response.put("status", httpStatus.value());
        response.put("message", message);
        response.put("error", error);
        response.put("data", data);

        return new ResponseEntity<>(response, httpStatus);
    }
}
