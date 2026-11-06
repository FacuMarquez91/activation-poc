
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';

import { Injectable } from '@nestjs/common';
import { Server, Socket } from 'socket.io';

@Injectable()
@WebSocketGateway({
  cors: {
    origin: 'http://localhost:5173',
    methods: ['GET', 'POST'],
  },
})
export class ActivationGateway
  implements
    OnGatewayConnection,
    OnGatewayDisconnect
{
  @WebSocketServer()
  server!: Server;

  handleConnection(client: Socket) {
    console.log(
      `WebSocket conectado: ${client.id}`,
    );
  }

  handleDisconnect(client: Socket) {
    console.log(
      `WebSocket desconectado: ${client.id}`,
    );
  }

  @SubscribeMessage('watch-activation')
  watchActivation(
    @ConnectedSocket() client: Socket,
    @MessageBody() activationId: string,
  ) {
    if (
      typeof activationId !== 'string' ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
        activationId,
      )
    ) {
      return { error: 'activationId invalido' };
    }

    client.join(`activation:${activationId}`);

    console.log(
      `Cliente ${client.id} sigue ${activationId}`,
    );

    return {
      watching: activationId,
    };
  }

  @SubscribeMessage('unwatch-activation')
  unwatchActivation(
    @ConnectedSocket() client: Socket,
    @MessageBody() activationId: string,
  ) {
    if (typeof activationId === 'string') {
      client.leave(`activation:${activationId}`);
    }
  }

  publishUpdate(
    activationId: string,
    data: unknown,
  ) {
    this.server
      .to(`activation:${activationId}`)
      .emit('activation:updated', data);
  }
}
