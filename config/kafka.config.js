module.exports = {
  clientId: "order-processing-system",
  brokers: ["localhost:9092"],
  topics: {
    orders: "orders",
    ordersRetry: "orders-retry",
    ordersDLQ: "orders-dlq",
  },
  consumer: {
    groupId: "order-consumer-group",
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  },
  retry: {
    maxRetries: 3,
    retryDelayMs: 2000,
  },
};
