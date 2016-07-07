package org.gooru.jobs

import groovy.util.logging.Slf4j
import org.gooru.jobs.config.Configuration
import org.gooru.jobs.processors.JobProcessor

/**
 * @author ashish on 6/7/16.
 */
@Slf4j("LOGGER")
class EntryPoint {

    public static void main(String[] args) {
        LOGGER.debug "Starting the application ..."
        def mainClass = new EntryPoint()
        LOGGER.debug "Created the main class"

        mainClass.initializeSystem()

        mainClass.process()

        mainClass.finalizeSystem()

        LOGGER.debug "Done with processing"
    }

    private def process() {
        JobProcessor.process()
    }

    private def initializeSystem() {
        LOGGER.debug "Initializing the system"
        initializeConfiguration()
        LOGGER.debug "System initialized"
    }

    private def finalizeSystem() {
        LOGGER.debug "Finalizing system"

        LOGGER.debug "System finalized"
    }

    private def initializeConfiguration() {
        Configuration.instance.initialize()
        LOGGER.debug "Configuration db url : '{}", Configuration.instance.getDbUrl()
        LOGGER.debug "Configuration db user : '{}", Configuration.instance.getDbUser()
        LOGGER.debug "Configuration batch size : '{}", Configuration.instance.getBatchSize()
        LOGGER.debug "Configuration db thread pool size : '{}", Configuration.instance.getThreadPoolSize()
        LOGGER.debug "Configuration http timeout: '{}'", Configuration.instance.getHttpTimeout()
    }
}
