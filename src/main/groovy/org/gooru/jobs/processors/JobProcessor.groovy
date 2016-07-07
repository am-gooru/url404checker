package org.gooru.jobs.processors

import groovy.util.logging.Slf4j
import org.gooru.jobs.config.Configuration

/**
 * @author ashish on 6/7/16.
 */
@Slf4j("LOGGER")
class JobProcessor {

    static def process() {
        LOGGER.debug "Starting job processor"
        def i = 0
        Map context = [offset: 0, limit: Configuration.instance.getBatchSize()]
        while (i++ < 20) {
            LOGGER.debug "context is '{}' ", context
            List input = new DbScanner().scan(context)
            if (input) {
                LOGGER.debug "input size is '{}'", input.size()
                context.offset += context.limit
                List result = new StatusChecker().checkStatus(input)

                new DbStatusUpdater().updateStatus(result)
            } else {
                // Did not find anything. Clean things up as next time we have to start from scratch
                context.offset = 0
                LOGGER.info "Done with existing processing, now going to wait for '{}' seconds", Configuration.instance.getIntervalBetweenJobs()
                sleep(Configuration.instance.getIntervalBetweenJobs() * 1000)
            }
        }
    }
}
