package com.shirin.order.application;

import lombok.AllArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@AllArgsConstructor
public class OrderNumberSequenceRepository {

    private final JdbcTemplate jdbcTemplate;
    public long nextValue() {
        Long value = jdbcTemplate.queryForObject(
                "SELECT nextval('order_number_seq')",
                Long.class
        );

        if (value == null) {
            throw new IllegalStateException("Could not get next order number sequence value");
        }

        return value;
    }
}
