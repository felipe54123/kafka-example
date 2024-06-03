// index.js del consumidor

import { Kafka } from "kafkajs";
import nodemailer from "nodemailer";
import express from "express";
import cron from "node-cron";

const kafka = new Kafka({
    clientId: "consumer",
    brokers: ["kafka:9092"],
});

const consumer = kafka.consumer({ groupId: "consumer-group" });

const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: 'superfelipe.ff@gmail.com',
        pass: 'diml pmfc stci hexa',
    },
});

const app = express();
const port = 3000;

let latestMessages = {};

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Manejar solicitudes GET por ID
app.get("/:id", (req, res) => {
    const id = req.params.id;
    const message = latestMessages[id];
    if (message) {
        res.json(message);
    } else {
        res.sendStatus(404);
    }
});

app.post("/", async (req, res) => {
    const value = req.body.value;

    try {
        console.log('Received value:', value);

        await transporter.sendMail({
            from: 'superfelipe.ff@gmail.com',
            to: 'felipe.fernandez1@mail.udp.cl',
            subject: 'Estadísticas de Kafka',
            text: `Valor recibido: ${value}`,
        });

        await sendConfirmationToProducer(value);

        // Aquí puedes programar la tarea para reenviar el mensaje después de un tiempo N
        scheduleMessageResend(value);

        res.sendStatus(200);
    } catch (error) {
        console.error('Error processing value:', error);
        res.sendStatus(500);
    }
});

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: "calc-stats", fromBeginning: true });

    await consumer.run({
        eachMessage: async function({ message }) {
            const { value, state } = JSON.parse(message.value.toString());
            const id = message.offset;

            latestMessages[id] = { value, state, timestamp: new Date() };

            console.log('Received value from Kafka:', value);

            await transporter.sendMail({
                from: 'superfelipe.ff@gmail.com',
                to: 'felipe.fernandez1@mail.udp.cl',
                subject: 'Estadísticas de Kafka',
                text: `Valor recibido de Kafka: ${value}, Estado: ${state}`,
            });

            await sendConfirmationToProducer(value);

            // Aquí puedes programar la tarea para reenviar el mensaje después de un tiempo N
            scheduleMessageResend(value);
        }
    });
};

const sendConfirmationToProducer = async (value) => {
    console.log('Sending confirmation to producer:', value);
    // Aquí puedes implementar la lógica para enviar una confirmación al productor
};

const scheduleMessageResend = (value) => {
    console.log('Scheduling message resend:', value);
    // Programa una tarea para reenviar el mensaje después de un tiempo N usando node-cron
    cron.schedule('*/5 * * * *', async () => {
        // Aquí puedes reenviar el mensaje a la cola con el estado actualizado
        await resendMessageToQueue(value);
    });
};

const resendMessageToQueue = async (value) => {
    console.log('Resending message to queue:', value);
    // Aquí puedes reenviar el mensaje a la cola con el estado actualizado
};

run().catch(console.error);

app.listen(port, () => {
    console.log(`Consumer app listening on port ${port}`);
});
