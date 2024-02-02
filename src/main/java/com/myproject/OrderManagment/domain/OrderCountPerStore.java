package com.myproject.OrderManagment.domain;

public record OrderCountPerStore(String locationId,
                                 Long orderCount) {
}
