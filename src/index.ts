import express, { Application } from 'express';
import OrderProducer, { OrderStatus } from './OrderProducer';
import DeliveryBalconyConsumer from './DeliveryBalconyConsumer';
import { v4 as uuidv4 } from 'uuid';
import redis, { RedisClient } from 'redis';
import { promisify } from 'util';
import MotoboyProducer from './MotoboyProducer';

const PORT = process.env.PORT || 4000;

const app: Application = express();
app.use(express.json());

export type AsyncRedisClient = RedisClient & {
  getAsync?: (key: string) => Promise<string>,
  setAsync?: (key: string, value: string) => Promise<string>,
  getsetAsync?: (key: string, value: string) => Promise<string>,
};

const redisClient: AsyncRedisClient = redis.createClient({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT) || 6379,
  auth_pass: process.env.REDIS_PASSWORD || undefined,
});
redisClient.getAsync = promisify(redisClient.get).bind(redisClient);
redisClient.setAsync = promisify(redisClient.set).bind(redisClient);
redisClient.getsetAsync = promisify(redisClient.getset).bind(redisClient);

const orderProducer = new OrderProducer();
orderProducer.start();

const motoboyProducer = new MotoboyProducer();
motoboyProducer.start();

const deliveryBalconyConsumer = new DeliveryBalconyConsumer(redisClient, motoboyProducer);
deliveryBalconyConsumer.start();

app.post('/order', async (req, res) => {
  try {
    const order = { ...req.body, id: uuidv4() };
    await redisClient.setAsync(`${order.id}-status`, OrderStatus.WAITING);
    await redisClient.setAsync(order.id, JSON.stringify(order));
    await orderProducer.sendOrder(order);
    res.send('Order sent!');
  } catch (error) {
    res.send(error);
  }
});

app.listen(PORT, () => {
  console.log(`Delivery App is listening at http://localhost:${PORT}`)
});

process.on('exit', () => {
  orderProducer.close();
  motoboyProducer.close();
  deliveryBalconyConsumer.close();
});
