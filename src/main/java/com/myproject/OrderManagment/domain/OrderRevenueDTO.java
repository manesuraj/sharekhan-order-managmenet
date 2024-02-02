package com.myproject.OrderManagment.domain;

public record OrderRevenueDTO(
        String locationId,
        OrderType orderType,
        TotalRevenue totalRevenue
) {
}
