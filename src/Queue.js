'use strict'

const Arena = require('bull-arena')
const basicAuth = require('express-basic-auth')

const Bull = require('bull')
const humanInterval = require('human-interval')

const fs = require('fs')
const differenceInMilliseconds = require('date-fns/differenceInMilliseconds')
const parseISO = require('date-fns/parseISO')

class Queue {
  constructor(Logger, Config, jobs, app, resolver) {
    this.Logger = Logger
    this.jobs = jobs

    this.app = app
    this.resolver = resolver

    this._queues = null

    const { connection, arena, ...connections } = Config.get('bull')

    this.config = connections[connection]
    this.arenaConfig = arena
    this.connections = connections
  }

  _getJobListeners(Job) {
    const jobListeners = Object.getOwnPropertyNames(Job.prototype)
      .filter((method) => method.startsWith('on'))
      .map((method) => {
        const eventName = method
          .replace(/^on(\w)/, (match, group) => group.toLowerCase())
          .replace(/([A-Z]+)/, (match, group) => ` ${group.toLowerCase()}`)

        return { eventName, method }
      })
    return jobListeners
  }

  get queues() {
    if (!this._queues) {
      this._queues = this.jobs.reduce((queues, path) => {
        const Job = this.app.use(path)

        let config = this.config
        if (Job.connection) {
          config = this.connections[Job.connection]
        }

        queues[Job.key] = {
          bull: new Bull(Job.key, config),
          Job,
          name: Job.key,
          concurrency: Job.concurrency || 1,
          options: Job.options,
        }

        return queues
      }, {})
    }

    return this._queues
  }

  get(name) {
    return this.queues[name]
  }

  add(name, method, data, options) {
    const queue = this.get(name)

    const job = queue.bull.add(method, data, { ...queue.options, ...options })

    return job
  }

  schedule(name, data, date, options) {
    let delay

    if (typeof date === 'number' || date instanceof Number) {
      delay = date
    } else {
      if (typeof date === 'string' || date instanceof String) {
        const byHuman = humanInterval(date)
        if (!isNaN(byHuman)) {
          delay = byHuman
        } else {
          delay = differenceInMilliseconds(parseISO(date), new Date())
        }
      } else {
        delay = differenceInMilliseconds(date, new Date())
      }
    }

    if (delay > 0) {
      return this.add(name, data, { ...options, delay })
    } else {
      throw new Error('Invalid schedule time')
    }
  }

  ui() {
    const queues = Object.values(this.queues).map((queue) => {
      const Job = new queue.Job()
      return {
        name: queue.Job.key,
        hostId: Job.constructor.name,
        host: this.config.host,
        port: this.config.port || 6379,
        password: this.config.password || null,
        type: 'bull',
        prefix: this.config.keyPrefix || 'bull',
      }
    })

    const port = this.arenaConfig.port || 1212
    const arenaObj = Arena(
      {
        queues,
      },
      {
        basePath: this.arenaConfig.prefix || '/',
        disableListen: true,
        useCdn: false,
        port,
      }
    )

    const express = require('express')
    const app = express()

    // for health check
    app.get('/hello', function (req, res) {
      res.send('hello')
    })

    if (this.arenaConfig.user && this.arenaConfig.password) {
      const basicAuthConfig = { challenge: true, users: {} }
      basicAuthConfig.users[this.arenaConfig.user || 'admin'] =
        this.arenaConfig.password || ''

      app.use(basicAuth(basicAuthConfig))
    }

    // Make arena's resources (js/css deps) available at the base app route
    app.use('/', arenaObj)

    const server = app.listen(port, () => {
      this.Logger.info('Bull arena is listening at: %s', port)
    })

    const shutdown = () => {
      server.close(() => {
        this.Logger.info('Stopping bull board server')
        process.exit(0)
      })
    }

    process.on('SIGTERM', shutdown)
    process.on('SIGINT', shutdown)
  }

  async remove(name, jobId) {
    const job = await this.queues[name].bull.getJob(jobId)

    job.remove()
  }

  /* eslint handle-callback-err: "error" */
  handleException(error, job) {
    let handler
    try {
      const exceptionHandlerFile = this.resolver
        .forDir('exceptions')
        .getPath('QueueHandler.js')
      fs.accessSync(exceptionHandlerFile, fs.constants.R_OK)

      const namespace = this.resolver
        .forDir('exceptions')
        .translate('QueueHandler')
      handler = this.app.make(this.app.use(namespace))
    } catch (err) {
      this.Logger.error(`name=${job.queue.name} id=${job.id}`)
    }
    if (handler) handler.handleError(error, job)
    else {
      this.Logger.error(`name=${job.queue.name} id=${job.id}`)
      this.Logger.error('%o', error)
      throw error
    }
  }

  process() {
    this.Logger.info('Queue processing started')
    Object.values(this.queues).forEach((queue) => {
      const Job = new queue.Job()

      const jobListeners = this._getJobListeners(queue.Job)

      jobListeners.forEach(function (item) {
        queue.bull.on(item.eventName, Job[item.method].bind(Job))
      })

      queue.bull.process('*', queue.concurrency, async (job) => {
        this.Logger.info('Received new job %s', job.name)
        try {
          if (job.name && Job[job.name]) return await Job[job.name](job)
          if (Job.defaultHandle) return await Job.defaultHandle(job)
        } catch (error) {
          if (Job.onError) Job.onError(error, job)
          else this.handleException(error, job)
        }
      })
    })

    const shutdown = () => {
      const promises = Object.values(this.queues).map((queue) => {
        return queue.bull.close()
      })

      return Promise.all(promises).then(process.exit(0))
    }

    process.on('SIGTERM', shutdown)
    process.on('SIGINT', shutdown)

    return this
  }
}

module.exports = Queue
