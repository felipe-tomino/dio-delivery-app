import { Producer, ProducerGlobalConfig } from 'node-rdkafka';

export enum OrderStatus {
  WAITING = 'WAITING',
  FOOD_READY = 'FOOD_READY',
  DRINKS_READY = 'DRINKS_READY',
  DONE = 'DONE',
}

export type Order = {
  id: string,
  address: number,
  food: string[],
  drinks: string[],
  status: OrderStatus,
}

export default class OrderProducer extends Producer {
  constructor() {
    const config: ProducerGlobalConfig = process.env.KAFKA_PASSWORD
      ? {
        'metadata.broker.list': process.env.KAFKA_BROKER_URI || 'localhost:9092',
        'dr_cb': true,
        'sasl.username': process.env.KAFKA_USERNAME,
        'sasl.password': process.env.KAFKA_PASSWORD,
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'socket.keepalive.enable': true,
        'debug': 'generic,broker,security',
        'security.protocol': 'sasl_ssl',
      }
      : {
        'metadata.broker.list': process.env.KAFKA_BROKER_URI || 'localhost:9092',
        'dr_cb': true,
      };
    super(config, {});
    super
      .on('ready', () => console.log('Started OrderProducer'))
      .on('event.error', (error) => { throw error });
  }

  start() {
    super.connect();
  }

  close() {
    super.disconnect();
  }

  async sendOrder(order: Order) {
    const { food, drinks, ...rest } = order;
    if (food.length) {
      await super.produce(`${process.env.KAFKA_TOPIC_PREFIX || ''}food`, null, Buffer.from(JSON.stringify({ food, ...rest })));
      console.log('Food order sent to the kitchen!');
    }
    if (drinks.length) {
      await super.produce(`${process.env.KAFKA_TOPIC_PREFIX || ''}drinks`, null, Buffer.from(JSON.stringify({ drinks, ...rest })));
      console.log('Drinks order sent to the bar!');
    }
  }
}
