// index.js del productor

import { Kafka } from "kafkajs";
import express from "express";
import fs from "fs";

const kafka = new Kafka({
    clientId: "producer",
    brokers: ["kafka:9092"],
});

const producer = kafka.producer();
const app = express();
const port = 3001;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const sendMessageWithState = async (value) => {
    try {
        await producer.send({
            topic: "calc-stats",
            messages: [{ 
                value: JSON.stringify({ value, state: "recibido" })
            }],
        });
    } catch (error) {
        console.error('Error:', error);
    }
};

app.post("/", async (req, res) => {
    const value = req.body.value;

    if (isNaN(value)) {
        res.sendStatus(400);
    } else {
        await sendMessageWithState(value);
        res.sendStatus(200);
    }
});

app.get("/send-dataset", async (req, res) => {
    try {
        const datasetPath = "./dataset.json";
        const data = fs.readFileSync(datasetPath, 'utf8');
        const dataset = JSON.parse(data);
        
        for (const entry of dataset) {
            await sendMessageWithState(entry.value);
        }
        
        res.sendStatus(200);
    } catch (error) {
        console.error('Error:', error);
        res.sendStatus(500);
    }
});

// Enviar varios datos en intervalos de tiempo
app.post("/send-interval", async (req, res) => {
    const { values, interval } = req.body;

    if (!Array.isArray(values) || isNaN(interval)) {
        return res.sendStatus(400);
    }

    for (const value of values) {
        if (isNaN(value)) {
            continue;
        }
        await sendMessageWithState(value);
        await new Promise(resolve => setTimeout(resolve, interval));
    }

    res.sendStatus(200);
});

app.listen(port, async () => {
    await producer.connect();
    console.log(`Producer app listening on port ${port}`);
});
