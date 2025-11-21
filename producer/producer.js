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

const producer = kafka.producer();

const products = [
  "Laptop",
  "Mouse",
  "Keyboard",
  "Monitor",
  "Headphones",
  "Webcam",
  "Printer",
  "CPU"
];

function generateOrder(orderId) {
  return {
    orderId: `ORD-${orderId}`,
    product: products[Math.floor(Math.random() * products.length)],
    price: parseFloat((Math.random() * 1000 + 10).toFixed(2)),
  };
}

function serializeOrder(order) {
  return orderSchema.toBuffer(order);
}

async function produceOrders(count = 10, intervalMs = 2000) {
  await producer.connect();
  console.log("Producer connected to Kafka");

  let orderId = 1000;

  const interval = setInterval(async () => {
    try {
      const order = generateOrder(orderId++);
      const serializedOrder = serializeOrder(order);

      await producer.send({
        topic: config.topics.orders,
        messages: [
          {
            key: order.orderId,
            value: serializedOrder,
            headers: {
              timestamp: Date.now().toString(),
            },
          },
        ],
      });

      console.log(
        `Produced order: ${order.orderId} - ${order.product} - $${order.price}`
      );

      if (orderId > 1000 + count) {
        clearInterval(interval);
        console.log("\nFinished producing orders");
        await producer.disconnect();
      }
    } catch (error) {
      console.error("Error producing message:", error);
    }
  }, intervalMs);
}

process.on("SIGINT", async () => {
  console.log("\nShutting down producer...");
  await producer.disconnect();
  process.exit(0);
});

const orderCount = process.argv[2] || 20;
const interval = process.argv[3] || 2000;

console.log(
  `Starting producer (${orderCount} orders, ${interval}ms interval)...\n`
);
produceOrders(orderCount, interval);
