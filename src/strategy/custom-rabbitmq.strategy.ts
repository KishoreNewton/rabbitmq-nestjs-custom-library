import {
  CustomTransportStrategy,
  PacketId,
  ReadPacket,
  Server,
  WritePacket,
} from '@nestjs/microservices';
import * as amqp from 'amqplib';

export class RabbitMQTransportServer
  extends Server
  implements CustomTransportStrategy
{
  private server: amqp.Connection = null;
  private channel: amqp.Channel = null;

  constructor(private readonly url: string, private readonly queue: string) {
    super();
  }

  private async init() {
    this.server = await amqp.connect(this.url);
    this.channel = await this.server.createChannel();
    this.channel.assertQueue(`${this.queue}_sub`, { durable: true });
    this.channel.assertQueue(`${this.queue}_pub`, { durable: true });
  }

  public async listen(callback: () => void) {
    await this.init();
    this.channel.consume(`${this.queue}_sub`, this.handleMessage.bind(this), {
      noAck: true,
    });
    callback();
  }

  public async handleMessage(message: amqp.Message) {
    const { content } = message;
    const packet = JSON.parse(content.toString()) as ReadPacket & PacketId;
    const handler = this.messageHandlers[JSON.stringify(packet.pattern)];
    const NO_PATTERN_MESSAGE = 'fixLater';
    if (!handler) {
      return this.sendMessage({
        id: packet.id,
        err: NO_PATTERN_MESSAGE,
      });
    }
  }

  private sendMessage(packet: WritePacket & PacketId) {
    const buffer = Buffer.from(JSON.stringify(packet));
    this.channel.sendToQueue(`${this.queue}_pub`, buffer);
  }

  public close() {
    this.channel && this.channel.close();
    this.channel && this.server.close();
  }
}
