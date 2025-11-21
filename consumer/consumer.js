const { Kafka } = require("kafkajs");
const avro = require("avsc");
const fs = require("fs");
const path = require("path");
const config = require("../config/kafka.config");

const schemaPath = path.join(__dirname, "../schemas/order.avsc");
const orderSchema = avro.Type.forSchema(
  JSON.parse(fs.readFileSync(schemaPath, "utf-8"))
);

const kafka = new Kafka({
  clientId: config.clientId,
  brokers: config.brokers,
});

const consumer = kafka.consumer({
  groupId: config.consumer.groupId,
  sessionTimeout: config.consumer.sessionTimeout,
  heartbeatInterval: config.consumer.heartbeatInterval,
});

const producer = kafka.producer();

const priceAggregation = {
  totalPrice: 0,
  orderCount: 0,
  runningAverage: 0,
};

const retryMap = new Map();

function deserializeOrder(buffer) {
  return orderSchema.fromBuffer(buffer);
}

function updateAggregation(price) {
  priceAggregation.totalPrice += price;
  priceAggregation.orderCount++;
  priceAggregation.runningAverage =
    priceAggregation.totalPrice / priceAggregation.orderCount;

  console.log(
    `Running Average: $${priceAggregation.runningAverage.toFixed(2)} (${
      priceAggregation.orderCount
    } orders)`
  );
}

async function processOrder(order, key) {
  const shouldFail = Math.random() < 0.2;

  if (shouldFail) {
    throw new Error("Simulated processing failure");
  }

  await new Promise((resolve) => setTimeout(resolve, 100));

  updateAggregation(order.price);

  console.log(
    `Processed: ${order.orderId} - ${order.product} - $${order.price}`
  );
}

async function sendToDLQ(message, error) {
  try {
    await producer.send({
      topic: config.topics.ordersDLQ,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            error: error.message,
            failedAt: Date.now().toString(),
            originalTopic: config.topics.orders,
          },
        },
      ],
    });
    console.log(`Sent to DLQ: ${message.key.toString()}`);
  } catch (dlqError) {
    console.error("Failed to send to DLQ:", dlqError);
  }
}

async function sendToRetryTopic(message, retryCount) {
  try {
    await producer.send({
      topic: config.topics.ordersRetry,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            retryCount: retryCount.toString(),
            retryAt: Date.now().toString(),
          },
        },
      ],
    });
    console.log(
      `Sent to retry queue (attempt ${retryCount}): ${message.key.toString()}`
    );
  } catch (retryError) {
    console.error("Failed to send to retry topic:", retryError);
  }
}

async function handleMessage(message) {
  const key = message.key.toString();
  let retryCount = retryMap.get(key) || 0;

  try {
    const order = deserializeOrder(message.value);
    console.log(
      `Received: ${order.orderId} - ${order.product} - $${order.price}`
    );

    await processOrder(order, key);

    retryMap.delete(key);
  } catch (error) {
    console.error(`Error processing ${key}: ${error.message}`);

    retryCount++;
    retryMap.set(key, retryCount);

    if (retryCount <= config.retry.maxRetries) {
      console.log(
        `Retry ${retryCount}/${config.retry.maxRetries} for ${key}`
      );
      await sendToRetryTopic(message, retryCount);
    } else {
      console.log(`Max retries exceeded for ${key}`);
      await sendToDLQ(message, error);
      retryMap.delete(key);
    }
  }
}

async function startConsumer() {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({
    topics: [config.topics.orders, config.topics.ordersRetry],
    fromBeginning: false,
  });

  console.log("Consumer connected and subscribed");
  console.log(
    `📡 Listening to topics: ${config.topics.orders}, ${config.topics.ordersRetry}\n`
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await handleMessage(message);
    },
  });
}

process.on("SIGINT", async () => {
  console.log("\nShutting down consumer...");
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
});

console.log("Starting consumer...\n");
startConsumer().catch(console.error);
