import { type Consumer } from 'nats'
import { mkdir, writeFile } from 'fs/promises'
import path from 'path'
import { STORE_DIR } from './config'
import { initDb } from './surreal'
import { RecordId } from 'surrealdb.js'

export const processEventMessages = async (consumer: Consumer) => {
  const eventMessages = await consumer.fetch({
    max_messages: 128,
    expires: 1000,
  })
  const db = await initDb()
  if (!db) {
    return
  }

  const now = new Date()

  for await (const message of eventMessages) {
    const companyId = message.headers!.get('Company-ID').replaceAll('-', '')
    const eventTimestamp = new Date(message.headers!.get('Event-DateTime'))
    const employeeId = message.headers!.get('Event-EmployeeNoString')
    const deviceName = message.headers!.get('Event-DeviceName')
    const requestTimestamp = new Date(message.headers!.get('Request-DateTime'))

    const imageFilename = generateImageFilename(
      eventTimestamp,
      employeeId,
      deviceName,
    )
    const savePath = path.join(
      STORE_DIR,
      companyId,
      imageFilename.substring(0, 8),
      imageFilename,
    )
    await mkdir(path.dirname(savePath), { recursive: true })
    await writeFile(savePath, message.data)

    const company = new RecordId('company', companyId)
    await db.create<any>('hikvision_event', {
      company,
      employee_id: employeeId,
      device_name: deviceName,
      event_time: eventTimestamp,
      request_time: requestTimestamp,
      created_at: now,
      updated_at: now,
    })
    message.ack()
  }
}

function generateImageFilename(
  eventTime: Date,
  employeeId: string,
  deviceName: string,
): string {
  const formattedTime = formatDateToSlug(eventTime)
  return `${formattedTime}-${employeeId}-${deviceName}.jpg`
}

function formatDateToSlug(date: Date): string {
  const dateFormatter = new Intl.DateTimeFormat('en-GB', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })

  const [formattedDate, formattedTime] = dateFormatter.format(date).split(', ')
  const [day, month, year] = formattedDate.split('/')
  return `${year}${month}${day}-${formattedTime.replaceAll(':', '')}`
}
