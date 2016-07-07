package org.gooru.jobs.processors

import groovy.util.logging.Slf4j
import org.gooru.jobs.db.DbConnectionHelper

/**
 * @author ashish on 6/7/16.
 */
@Slf4j("LOGGER")
class DbScanner {

    // Look for the applicable records in db for which we need to run the job

    private static String query = 'select id, url from url_checker where job_status is null'

    def scan(Map context) {
        LOGGER.debug "Starting scan with batch size of '{}' and offset '{}'", context.limit, context.offset

        def db = DbConnectionHelper.getDbConnection()
        LOGGER.debug "Got Db connection"

        List result = db.rows(query, context.offset, context.limit)

        LOGGER.debug "Closing Db connection"
        DbConnectionHelper.closeDbConnection(db)

        result
    }
}
