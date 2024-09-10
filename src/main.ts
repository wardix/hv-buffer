import { connect, type Consumer } from 'nats'
import { NATS_SERVERS, NATS_TOKEN } from './config'
import { processEventMessages } from './message-processor'

async function consumeMessages() {
  const nc = await connect({
    servers: NATS_SERVERS,
    token: NATS_TOKEN,
  })

  const js = nc.jetstream()
  const c: Consumer = await js.consumers.get(
    'EVENTS',
    'hikvision_buffer_processor',
  )

  setInterval(async () => {
    processEventMessages(c)
  }, 4000)

  process.on('SIGINT', async () => {
    await nc.drain()
    process.exit()
  })
}

consumeMessages().catch((err) => {
  console.error('Error consuming messages:', err)
})
