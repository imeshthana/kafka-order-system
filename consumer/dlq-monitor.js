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
  groupId: "dlq-monitor-group",
});

function deserializeOrder(buffer) {
  return orderSchema.fromBuffer(buffer);
}

async function startDLQMonitor() {
  await consumer.connect();
  await consumer.subscribe({
    topic: config.topics.ordersDLQ,
    fromBeginning: true,
  });

  console.log("DLQ Monitor connected");
  console.log(`Monitoring DLQ: ${config.topics.ordersDLQ}\n`);
  console.log("═══════════════════════════════════════════════════════════");

  let dlqCount = 0;

  await consumer.run({
    eachMessage: async ({ message }) => {
      dlqCount++;

      try {
        const order = deserializeOrder(message.value);
        const error = message.headers.error?.toString() || "Unknown error";
        const failedAt = message.headers.failedAt?.toString() || "Unknown";
        const originalTopic =
          message.headers.originalTopic?.toString() || "Unknown";

        console.log(`\nDLQ Message #${dlqCount}`);
        console.log(`   Order ID: ${order.orderId}`);
        console.log(`   Product: ${order.product}`);
        console.log(`   Price: $${order.price}`);
        console.log(`   Error: ${error}`);
        console.log(
          `   Failed At: ${new Date(parseInt(failedAt)).toISOString()}`
        );
        console.log(`   Original Topic: ${originalTopic}`);
        console.log(
          "───────────────────────────────────────────────────────────"
        );
      } catch (err) {
        console.error("Error processing DLQ message:", err);
      }
    },
  });
}

process.on("SIGINT", async () => {
  console.log("\nShutting down DLQ monitor...");
  await consumer.disconnect();
  process.exit(0);
});

console.log("Starting DLQ Monitor...\n");
startDLQMonitor().catch(console.error);
