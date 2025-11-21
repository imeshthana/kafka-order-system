const { Kafka } = require("kafkajs");
const config = require("../config/kafka.config");

const kafka = new Kafka({
  clientId: config.clientId,
  brokers: config.brokers,
});

const admin = kafka.admin();

async function setupTopics() {
  try {
    await admin.connect();
    console.log("Connected to Kafka admin");

    const existingTopics = await admin.listTopics();
    console.log("\nExisting topics:", existingTopics);

    const topicsToCreate = [
      {
        topic: config.topics.orders,
        numPartitions: 3,
        replicationFactor: 1,
      },
      {
        topic: config.topics.ordersRetry,
        numPartitions: 1,
        replicationFactor: 1,
      },
      {
        topic: config.topics.ordersDLQ,
        numPartitions: 1,
        replicationFactor: 1,
      },
    ];

    const newTopics = topicsToCreate.filter(
      (t) => !existingTopics.includes(t.topic)
    );

    if (newTopics.length > 0) {
      await admin.createTopics({
        topics: newTopics,
      });

      console.log("\nCreated topics:");
      newTopics.forEach((t) => {
        console.log(`   - ${t.topic} (${t.numPartitions} partitions)`);
      });
    } else {
      console.log("\nAll required topics already exist");
    }

    const finalTopics = await admin.listTopics();
    console.log("\nFinal topics:", finalTopics);
  } catch (error) {
    console.error("Error setting up topics:", error);
  } finally {
    await admin.disconnect();
    console.log("\nDisconnected from Kafka admin");
  }
}

setupTopics();
