'use strict'

const { Command } = require('@adonisjs/ace')
const Config = use('Config')

class Listen extends Command {
  static get inject() {
    return ['Rocketseat/Bull']
  }

  constructor(Bull) {
    super()
    this.Bull = Bull
  }

  static get signature() {
    return `
      queue:listen
      { --arena : Run bull arena dashboard }
    `
  }

  static get description() {
    return 'Start the Queue'
  }

  async handle(args, { arena }) {
    const bullConfig = Config.get('bull')
    let onBoot = bullConfig.onBoot
    if (onBoot === undefined || onBoot === null) onBoot = true
    if (onBoot) throw new Error('You cannot use the command while onBoot=true')

    this.Bull.process()
    if (arena) {
      this.Bull.ui()
    }
  }
}

module.exports = Listen
