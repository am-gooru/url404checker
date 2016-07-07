package org.gooru.jobs.config

import groovy.json.JsonSlurper
import groovy.transform.Synchronized
import groovy.util.logging.Slf4j

/**
 * @author ashish on 6/7/16.
 */
@Slf4j("LOGGER")
@Singleton
class Configuration {
    private volatile boolean initialized = false
    private def configuration

    @Synchronized
    public void initialize() {
        if (!initialized) {
            LOGGER.debug "Initializing the config from Json resource"
            InputStream stream = Configuration.classLoader.getResourceAsStream("config.json")
            configuration = new JsonSlurper().parse(stream)
            initialized = true
            LOGGER.debug "Initialization of config done"
        }
    }

    public def getDbUrl() {
        if (!initialized) {
            throw new IllegalStateException()
        }
        return configuration."db.url"

    }

    public def getDbUser() {
        if (!initialized) {
            throw new IllegalStateException()
        }
        return configuration."db.username"

    }

    public def getDbPassword() {
        if (!initialized) {
            throw new IllegalStateException()
        }
        return configuration."db.password"

    }

    public def getBatchSize() {
        if (!initialized) {
            throw new IllegalStateException()
        }
        return configuration."batch.size"

    }

    public def getThreadPoolSize() {
        if (!initialized) {
            throw new IllegalStateException()
        }
        return configuration."thread.pool.size"

    }

    public def getIntervalBetweenJobs() {
        if (!initialized) {
            throw new IllegalStateException()
        }
        return configuration."interval.between.jobs.seconds"

    }

    public def getHttpTimeout() {
        if (!initialized) {
            throw new IllegalStateException()
        }
        return configuration."http.timeout.seconds"
    }

    public def getDbPoolSize() {
        if (!initialized) {
            throw new IllegalStateException()
        }
        return configuration."db.pool.size"

    }

}
