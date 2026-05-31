package com.shirin.order.application;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;
import java.time.Year;

@Component
@AllArgsConstructor
public class OrderNumberGenerator {

    private final OrderNumberSequenceRepository sequenceRepository;
    public String generate() {
        long sequenceValue = sequenceRepository.nextValue();
        int year = Year.now().getValue();

        return "ORD-" + year + "-" + String.format("%06d", sequenceValue);
    }
}
