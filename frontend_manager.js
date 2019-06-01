'use strict'

const PACKAGE = require('./package.json')
const VERSION = PACKAGE.version

const STAGE = process.env.STAGE || 'test'
const LOCAL = process.env.LOCAL || false

const AWS = require('aws-sdk')
AWS.config.update({region:'us-east-1', correctClockSkew: true})
const SQS = new AWS.SQS()

let PARAMS = {}
let IOT

async function configAWS() {
  IOT = new AWS.IotData({endpoint: `${PARAMS.IOT_ENDPOINT_HOST}`})
}

console.log(`Worker Manager Version: ${VERSION} :: Stage: ${STAGE}`)
console.log(`ENV: ${JSON.stringify(process.env)}`)

// Logger intercept for easy logging in the future.
const LOGGER = {
    info: (msg, userId = 'unset') => {
      if (userId !== 'unset') {
        console.log(`Frontend: [${userId}] ${msg}`)
        let notificationTopic = `${STAGE}/frontend/${userId}`
        mqttSend(notificationTopic, {notification: {type: 'success', msg: msg}})
      } else {
        console.log(`Frontend: ${msg}`)
      }
    },
    error: (msg, userId = 'unset') => {
      if (userId !== 'unset') {
        console.error(`Frontend: [${userId}] ${msg}`)
        let notificationTopic = `${STAGE}/frontend/${userId}`
        mqttSend(notificationTopic, {notification: {type: 'error', msg: msg}})
      } else {
        console.error(`Frontend: ${msg}`)
      }
    },
    debug: (msg, userId = 'unset') => {
      if (userId !== 'unset') {
        console.log(`Frontend: [${userId}] ${msg}`)
      } else {
        console.log(`Frontend: ${msg}`)
      }
    }
  }

// Frontend Methods
const apiSwitch = {
  getIsys: {
    props: [],
    func: makeIsy,
    type: 'global'
  },
  getNodeServers: {
    props: ['id'],
    func: makeIsy,
    type: 'client'
  },
  addNodeServer: {
    props: ['profileNum', 'ns.url', 'ns.name', 'ns.language', 'isy.id', 'isy.isyOnline'],
    func: addNodeServer,
    type: 'workers'
  },
  removeNodeServer: {
    props: ['profileNum', 'isy.id'],
    func: removeNodeServer,
    type: 'workers'
  },
  startNodeServer: {
    props: ['profileNum', 'isy.id'],
    func: startStopNodeServers,
    type: 'workers'
  },
  stopNodeServer: {
    props: ['profileNum', 'isy.id'],
    func: startStopNodeServers,
    type: 'workers'
  },
  removenode: {
    props: ['profileNum', 'isy.id'],
    func: nsReq,
    type: 'ns'
  },
  customparams: {
    props: ['profileNum', 'isy.id'],
    func: nsReq,
    type: 'ns'
  },
  polls: {
    props: ['profileNum', 'isy.id'],
    func: nsReq,
    type: 'ns'
  },
  notices: {
    props: ['profileNum', 'isy.id'],
    func: nsReq,
    type: 'ns'
  }
}

const propExists = (obj, path) => {
  return !!path.split(".").reduce((obj, prop) => {
      return obj && obj[prop] ? obj[prop] : undefined;
  }, obj)
}

const verifyProps = (message, props) => {
  let confirm = {
    valid: true,
    missing: null
  }
  for (let prop of props) {
    if (!propExists(message, prop)) {
      confirm.valid = false
      confirm.missing = prop
      break
    }
  }
  return confirm
}

const checkCommand = (command) => apiSwitch[command] || null

// MQTT Send Method
async function mqttSend(topic, message, fullMsg = {}, qos = 0) {
  if (fullMsg.userId && fullMsg.clientId) {
    message.userId = fullMsg.userId
    message.topic = `${STAGE}/frontend/${fullMsg.userId}/${fullMsg.clientId}`
  }
  const payload = JSON.stringify(message)
  const iotMessage = {
    topic: topic,
    payload: payload,
    qos: qos
  }
  return IOT.publish(iotMessage).promise()
}

// Command Methods
async function makeIsy(cmd, fullMsg) {
  let data = fullMsg[cmd]
  let req = {
    fullResponse: data.fullResponse || undefined,
    [cmd]: {}
  }
  if (data.hasOwnProperty('id')) { req['id'] = data.id }
  return mqttSend(`${STAGE}/isy`, req, fullMsg)
}

async function addNodeServer(cmd, fullMsg) {
  let data = fullMsg[cmd]
  if (!data.isy.isyOnline) {
    LOGGER.error('ISY not online.', fullMsg.userId)
    return false
  }
  let req = {
    [cmd]: {
      userId: fullMsg.userId,
      development: data.development || false,
      id: data.isy.id,
      profileNum: data.profileNum,
      url: data.ns.url,
      name: data.ns.name,
      language: data.ns.language,
      version: data.ns.version,
      isyVersion: data.isy.isyData.firmware,
      oauth: data.ns.oauth || {},
      ingressRequired: data.ns.ingressRequired || false
    }
  }
  return mqttSend(`${process.env.STAGE}/workers`, req, fullMsg)
}

async function removeNodeServer(cmd, fullMsg) {
  let data = fullMsg[cmd]
  if (!data.isy.isyOnline) {
    LOGGER.error('ISY not online.', fullMsg.userId)
    return false
  }
  let req = {
    [cmd]: {
      profileNum: data.profileNum,
      id: data.isy.id,
      isyVersion: data.isy.isyData.firmware
    }
  }
  return mqttSend(`${process.env.STAGE}/workers`, req, fullMsg)
}

async function startStopNodeServers(cmd, fullMsg) {
  let data = fullMsg[cmd]
  if (!data.isy.isyOnline) {
    LOGGER.error('ISY not online.', fullMsg.userId)
    return false
  }
  let req = {[cmd]: data}
  return mqttSend(`${process.env.STAGE}/workers`, req, fullMsg)
}

async function nsReq(cmd, fullMsg) {
  let data = fullMsg[cmd]
  if (!data.isy.isyOnline) {
    LOGGER.error('ISY not online.', fullMsg.userId)
    return false
  }
  let req = {
    profileNum: data.profileNum,
    id: data.isy.id,
    [cmd]: data
  }
  return mqttSend(`${process.env.STAGE}/ns`, req, fullMsg)
}

// Message Processing
async function processMessage(message) {
  let props = verifyProps(message, ['userId', 'clientId'])
  if (!props.valid) {
      return LOGGER.error(`Request missing required property: ${props.missing} :: ${JSON.stringify(message)}`)
  }
  LOGGER.debug(JSON.stringify(message))
  for (let key in message) {
    if (['userId', 'clientId', 'seq'].includes(key)) { continue }
    try {
      let command = checkCommand(key)
      if (!command) { continue }
      let props = verifyProps(message[key], apiSwitch[key].props)
      if (!props.valid) {
        return LOGGER.error(`${key} was missing ${props.missing} :: ${JSON.stringify(message)}`, message.userId)
      }
      LOGGER.debug(`Processing command ${key} for ${message.userId}/${message.clientId}`, message.userId)
      let result = await command.func(key, message)
      if (result) {
        LOGGER.debug(`Sent ${key} for ${message.userId}/${message.clientId}`, message.userId)
      } else {
        LOGGER.error(`Failed to process ${key} for ${message.userId}/${message.clientId}`, message.userId)
      }
    } catch (err) {
        LOGGER.error(`${key} error :: ${err.stack}`, message.userId)
    }
  }
}

async function getMessages() {
  const params = {
    MaxNumberOfMessages: 10,
    QueueUrl: PARAMS.SQS_FRONTEND,
    WaitTimeSeconds: 10
  }
  let deletedParams = {
    Entries: [],
    QueueUrl: PARAMS.SQS_FRONTEND
  }
  try {
    LOGGER.info(`Getting messages...`)
    let data = await SQS.receiveMessage(params).promise()
    if (data.Messages) {
        LOGGER.info(`Got ${data.Messages.length} message(s)`)
        let tasks = []
        for (let message of data.Messages) {
          try {
            let body = JSON.parse(message.Body)
            let msg = body.msg
            LOGGER.info(`Got Message: ${JSON.stringify(msg)}`)
            tasks.push(processMessage(msg))
          } catch (err) {
            LOGGER.error(`Message not JSON: ${message.Body}`)
          }
          deletedParams.Entries.push({
              Id: message.MessageId,
              ReceiptHandle: message.ReceiptHandle
          })
        }
        let results = []
        for (let task of tasks) {
          results.push(await task)
        }
        let deleted = await SQS.deleteMessageBatch(deletedParams).promise()
        deletedParams.Entries = []
        LOGGER.info(`Deleted Messages: ${JSON.stringify(deleted)}`)
    } else {
      LOGGER.info(`No messages`)
    }
  } catch (err) {
    LOGGER.error(err.stack)
  }
}

async function getParameters(nextToken) {
  const ssm = new AWS.SSM()
  var ssmParams = {
    Path: `/pgc/${STAGE}/`,
    MaxResults: 10,
    Recursive: true,
    NextToken: nextToken,
    WithDecryption: true
  }
  let params = await ssm.getParametersByPath(ssmParams).promise()
  if (params.Parameters.length === 0) throw new Error(`Parameters not retrieved. Exiting.`)
  for (let param of params.Parameters) {
    PARAMS[param.Name.split('/').slice(-1)[0]] = param.Value
  }
  if (params.hasOwnProperty('NextToken')) {
    await getParameters(params.NextToken)
  }
}

async function startHealthCheck() {
  require('http').createServer(function(request, response) {
    if (request.url === '/health' && request.method ==='GET') {
        //AWS ELB pings this URL to make sure the instance is running smoothly
        let data = JSON.stringify({uptime: process.uptime()})
        response.writeHead(200, {'Content-Type': 'application/json'})
        response.write(data)
        response.end()
    }
  }).listen(3000)
}

async function main() {
  await getParameters()
  if (!PARAMS.SQS_NS) {
    LOGGER.error(`No Queue retrieved. Exiting.`)
    process.exit(1)
  }
  LOGGER.info(`Retrieved Parameters from AWS Parameter Store for Stage: ${STAGE}`)
  await configAWS()
  startHealthCheck()
  try {
    while (true) {
      await getMessages()
    }
  } catch(err) {
    LOGGER.error(err.stack)
    main()
  }

}

['SIGINT', 'SIGTERM'].forEach(signal => {
  process.on(signal, () => {
    LOGGER.debug('Shutdown requested. Exiting...')
    setTimeout(() => {
      process.exit()
    }, 500)
  })
})

main()
