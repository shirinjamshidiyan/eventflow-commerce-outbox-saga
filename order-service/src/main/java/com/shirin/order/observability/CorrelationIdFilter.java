package com.shirin.order.observability;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import java.io.IOException;
import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String HEADER_NAME = "X-Correlation-Id";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {

        UUID correlationId = resolveCorrelationId(request.getHeader(HEADER_NAME));

        try {
            MDC.put("correlationId", correlationId.toString());
            response.setHeader(HEADER_NAME, correlationId.toString());

            filterChain.doFilter(request, response);
        } finally {
            MDC.clear();
        }


    }

    private UUID resolveCorrelationId(String value) {
        if (value == null || value.isBlank()) {
            return UUID.randomUUID();
        }

        try {
            return UUID.fromString(value.trim());
        } catch (IllegalArgumentException ex) {
            return UUID.randomUUID();
        }
    }

}
